package com.datastax.logging.appender;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

/**
 * Main class that uses Cassandra to store log entries into.
 * 
 */
public class CassandraAppender extends AppenderSkeleton {

	// CF column names
	public static final String HOST_IP = "host_ip";
	public static final String HOST_NAME = "host_name";
	public static final String APP_NAME = "app_name";
	public static final String LOGGER_NAME = "logger_name";
	public static final String LEVEL = "level";
	public static final String CLASS_NAME = "class_name";
	public static final String FILE_NAME = "file_name";
	public static final String LINE_NUMBER = "line_number";
	public static final String METHOD_NAME = "method_name";
	public static final String MESSAGE = "message";
	public static final String NDC = "ndc";
	public static final String APP_START_TIME = "app_start_time";
	public static final String THREAD_NAME = "thread_name";
	public static final String THROWABLE_STR = "throwable_str_rep";
	public static final String TIMESTAMP = "log_timestamp";

	/**
	 * Keyspace name. Default: "Logging".
	 */
	private String keyspaceName = "Logging";
	private String columnFamily = "log_entries";
	private String appName = "default";

	private String dcName = "datacenter1";
	
	private PreparedStatement statement;

	private String replication = "{'class': 'SimpleStrategy', 'datacenter1' : 1};";

	private ConsistencyLevel consistencyLevelWrite = ConsistencyLevel.ONE;

	private AtomicBoolean clientInitialized = new AtomicBoolean(false);
	private Cluster cluster;
	private Session session;

	private static final String ip = getIP();
	private static final String hostname = getHostName();

	/**
	 * Cassandra comma separated hosts.
	 */
	private String hosts = "localhost";

	/**
	 * Cassandra port.
	 */
	private int port = 9042;
	private String username = "";
	private String password = "";

	public CassandraAppender() {
		LogLog.debug("Creating CassandraAppender");
	}

	/**
	 * {@inheritDoc}
	 */
	public void close() {
		System.out.println("shutting down");
		session.shutdown();
		cluster.shutdown();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.log4j.Appender#requiresLayout()
	 */
	public boolean requiresLayout() {
		return false;
	}

	/**
	 * Called once all the options have been set. Starts listening for clients
	 * on the specified socket.
	 */
	public void activateOptions() {
		// reset();
	}

	private synchronized void initClient() {

		// another thread has already initialized the client, nothing to do
		if (clientInitialized.get()) {
			return;
		}

		String[] contactPoints = hosts.split(",\\s*");

		// Just while we initialise the client, we must temporarily
		// disable all logging or else we get into an infinite loop
		Level globalThreshold = LogManager.getLoggerRepository().getThreshold();
		LogManager.getLoggerRepository().setThreshold(Level.OFF);

		try {
			try {
				// FIXME pick up dc from config
				cluster = Cluster
						.builder()
						.addContactPoints(contactPoints)
						.withPort(port)
						.withCredentials(username, password)
						.withLoadBalancingPolicy(
								new DCAwareRoundRobinPolicy(dcName)).build();

				Metadata metadata = cluster.getMetadata();
				System.out.printf("Connected to cluster: %s\n",
						metadata.getClusterName());
				for (Host host : metadata.getAllHosts()) {
					System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
							host.getDatacenter(), host.getAddress(),
							host.getRack());
				}
				session = cluster.connect();
			} catch (Exception e) {
				LogLog.error("Can't initialize cassandra connections", e);
				errorHandler.error("Can't initialize cassandra connections: "
						+ e);
			}
			try {
				setupSchema();
			} catch (Exception e) {
				LogLog.error("Error setting up cassandra logging schema", e);
				errorHandler
						.error("Error setting up cassandra logging schema: "
								+ e);
			}

			try {
				session.execute("use " + keyspaceName + ";");
			} catch (Exception e) {
				LogLog.error("Error setting keyspace", e);
				errorHandler.error("Error setting keyspace: " + e);
			}
		} finally {
			// make sure we re-enable logging, even if we errored during client
			// setup
			LogManager.getLoggerRepository().setThreshold(globalThreshold);
		}

		clientInitialized.set(true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void append(LoggingEvent event) {
		// We have to defer initialization of the client because
		// ITransportFactory
		// references some Hadoop classes which can't safely be
		// used until the logging infrastructure is fully set up. If we attempt
		// to
		// initialize the client earlier, it causes NPE's from the constructor
		// of
		// org.apache.hadoop.conf.Configuration
		// The initClient method is synchronized and includes a double check of
		// the client status, so we only do this once.
		if (!clientInitialized.get()) {
			initClient();
		}

		createAndExecuteQuery(event);
	}

	private void createAndExecuteQuery(LoggingEvent event) {

		HashMap<String, Object> queryList = new HashMap<String, Object>();

		queryList.put("APP_NAME", appName);
		queryList.put("HOST_IP", ip);
		queryList.put("HOST_NAME", hostname);
		queryList.put("LOGGER_NAME", event.getLoggerName());
		queryList.put("LEVEL", event.getLevel().toString());

		LocationInfo locInfo = event.getLocationInformation();
		if (locInfo != null) {
			queryList.put("CLASS_NAME", locInfo.getClassName());
			queryList.put("FILE_NAME", locInfo.getFileName());
			queryList.put("LINE_NUMBER", locInfo.getLineNumber());
			queryList.put("METHOD_NAME", locInfo.getMethodName());

		}

		queryList.put("MESSAGE", event.getRenderedMessage());
		queryList.put("NDC", event.getNDC());
		// FIXME???
		queryList.put("APP_START_TIME",
				new Long(LoggingEvent.getStartTime()).toString());
		queryList.put("THREAD_NAME", event.getThreadName());

		String[] throwableStrs = event.getThrowableStrRep();

		StringBuilder throwableConcat = new StringBuilder("");
		if (throwableStrs != null) {

			for (String throwableStr : throwableStrs) {
				throwableConcat.append(throwableStr);
			}

		}
		queryList.put("THROWABLE_STR", throwableConcat.toString());
		// FIXME???
		queryList.put("TIMESTAMP", new Long(event.getTimeStamp()).toString());

		String queryCols = StringUtils.join(queryList.keySet().toArray(), ",");
		String questionString = "?" + StringUtils.repeat(", ?", queryList.size() - 1);
		if (statement == null) {
			statement = session.prepare("INSERT INTO " + keyspaceName + "."
					+ columnFamily + " (" + queryCols + ") VALUES ("
					+ questionString + ")");
			statement.setConsistencyLevel(ConsistencyLevel
					.valueOf(consistencyLevelWrite.toString()));
		}
		BoundStatement bound = new BoundStatement(statement);
		session.execute(bound.bind(queryList.values().toArray()));
	}

	/**
	 * Create Keyspace and CF if they do not exist.
	 */
	private void setupSchema() throws IOException {

		String ksQuery = "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName
				+ " WITH replication = " + replication + ";";

		String cfQuery = "CREATE TABLE IF NOT EXISTS " + keyspaceName + "."
				+ columnFamily + " ( " + "host_ip text, " + "host_name text,"
				+ "app_name text," + "\"timestamp\" text,"
				+ "app_start_time text," + "class_name text,"
				+ "file_name text," + "level text," + "line_number text,"
				+ "logger_name text," + "message text," + "method_name text,"
				+ "ndc text," + "thread_name text," + "throwable_str text,"
				+ "PRIMARY KEY ((host_ip, host_name, app_name), \"timestamp\")"
				+ ") WITH CLUSTERING ORDER BY (\"timestamp\" DESC) AND"
				+ " bloom_filter_fp_chance=0.010000 AND"
				+ " caching='KEYS_ONLY' AND" + " comment='' AND"
				+ " dclocal_read_repair_chance=0.000000 AND"
				+ " gc_grace_seconds=864000 AND" + " index_interval=128 AND"
				+ " read_repair_chance=0.100000 AND"
				+ " replicate_on_write='true' AND"
				+ " populate_io_cache_on_flush='false' AND"
				+ " default_time_to_live=0 AND"
				+ " speculative_retry='NONE' AND"
				+ " memtable_flush_period_in_ms=0 AND"
				+ " compaction={'class': 'SizeTieredCompactionStrategy'} AND "
				+ " compression={'sstable_compression': 'LZ4Compressor'};";
		session.execute(ksQuery);
		session.execute(cfQuery);
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public String getHosts() {
		return hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getReplication() {
		return replication;
	}

	public void setReplication(String strategy) {
		replication = unescape(strategy);
	}

	public String getConsistencyLevelWrite() {
		return consistencyLevelWrite.toString();
	}

	public void setConsistencyLevelWrite(String consistencyLevelWrite) {
		try {
			this.consistencyLevelWrite = ConsistencyLevel
					.valueOf(unescape(consistencyLevelWrite));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Consistency level "
					+ consistencyLevelWrite
					+ " wasn't found. Available levels: "
					+ StringUtils.join(ConsistencyLevel.values(), ", ") + ".");
		}
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	private static final Charset charset = Charset.forName("UTF-8");

	// Serialize a string
	public static ByteBuffer toByteBuffer(String s) {
		if (s == null) {
			return null;
		}

		return ByteBuffer.wrap(s.getBytes(charset));
	}

	// serialize a UUID
	public ByteBuffer toByteBuffer(UUID uuid) {
		long msb = uuid.getMostSignificantBits();
		long lsb = uuid.getLeastSignificantBits();
		byte[] buffer = new byte[16];

		for (int i = 0; i < 8; i++) {
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		}
		for (int i = 8; i < 16; i++) {
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		}

		return ByteBuffer.wrap(buffer);
	}

	// serialize a long
	public ByteBuffer toByteBuffer(long longVal) {
		return ByteBuffer.allocate(8).putLong(0, longVal);
	}

	private static String getHostName() {
		String hostname = "unknown";

		try {
			InetAddress addr = InetAddress.getLocalHost();
			hostname = addr.getHostName();
		} catch (Throwable t) {

		}
		return hostname;
	}

	private static String getIP() {
		String ip = "unknown";

		try {
			InetAddress addr = InetAddress.getLocalHost();
			ip = addr.getHostAddress();
		} catch (Throwable t) {

		}
		return ip;
	}

	/**
	 * Strips leading and trailing '"' characters
	 * 
	 * @param b
	 *            - string to unescape
	 * @return String - unexspaced string
	 */
	private static String unescape(String b) {
		if (b.charAt(0) == '\"' && b.charAt(b.length() - 1) == '\"')
			b = b.substring(1, b.length() - 1);
		return b;
	}
}
