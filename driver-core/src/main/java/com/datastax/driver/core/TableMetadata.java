package com.datastax.driver.core;

import java.util.*;

import com.datastax.driver.core.transport.Codec;

import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.db.marshal.*;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Describes a Table.
 */
public class TableMetadata {

    public static final String CF_NAME               = "columnfamily_name";

    private static final String KEY_VALIDATOR        = "key_validator";
    private static final String COMPARATOR           = "comparator";
    private static final String VALIDATOR            = "default_validator";

    private static final String KEY_ALIASES          = "key_aliases";
    private static final String COLUMN_ALIASES       = "column_aliases";
    private static final String VALUE_ALIAS          = "value_alias";

    private static final String DEFAULT_KEY_ALIAS    = "key";
    private static final String DEFAULT_COLUMN_ALIAS = "column";
    private static final String DEFAULT_VALUE_ALIAS  = "value";

    private final KeyspaceMetadata keyspace;
    private final String name;
    // We use a linked hashmap because we will keep this in the order of a 'SELECT * FROM ...'.
    private final Map<String, ColumnMetadata> columns = new LinkedHashMap<String, ColumnMetadata>();
    private final List<ColumnMetadata> partitionKey = new ArrayList<ColumnMetadata>();
    private final List<ColumnMetadata> clusteringKey = new ArrayList<ColumnMetadata>();
    private final Options options;

    private TableMetadata(KeyspaceMetadata keyspace, String name, Options options) {
        this.keyspace = keyspace;
        this.name = name;
        this.options = options;
    }

    static TableMetadata build(KeyspaceMetadata ksm, CQLRow row) {
        try {
            String name = row.getString(CF_NAME);
            TableMetadata tm = new TableMetadata(ksm, name, new Options(row));

            // Partition key
            AbstractType kt = TypeParser.parse(row.getString(KEY_VALIDATOR));
            List<AbstractType<?>> keyTypes = kt instanceof CompositeType
                                           ? ((CompositeType)kt).types
                                           : Collections.<AbstractType<?>>singletonList(kt);
            List<String> keyAliases = fromJsonList(row.getString(KEY_ALIASES));
            for (int i = 0; i < keyTypes.size(); i++) {
                String cn = keyAliases.size() > i
                          ? keyAliases.get(i)
                          : (i == 0 ? DEFAULT_KEY_ALIAS : DEFAULT_KEY_ALIAS + (i + 1));
                DataType dt = Codec.rawTypeToDataType(keyTypes.get(i));
                ColumnMetadata colMeta = new ColumnMetadata(tm, cn, dt, null);
                tm.columns.put(cn, colMeta);
                tm.partitionKey.add(colMeta);
            }

            // Clustering key
            // TODO: this is actually more complicated than that ...
            AbstractType ct = TypeParser.parse(row.getString(COMPARATOR));
            boolean isComposite = ct instanceof CompositeType;
            List<AbstractType<?>> columnTypes = isComposite
                                              ? ((CompositeType)ct).types
                                              : Collections.<AbstractType<?>>singletonList(ct);
            List<String> columnAliases = fromJsonList(row.getString(COLUMN_ALIASES));
            int clusteringSize;
            boolean hasValue;
            if (isComposite) {
                if (columnTypes.size() == columnAliases.size()) {
                    hasValue = true;
                    clusteringSize = columnTypes.size();
                } else {
                    hasValue = false;
                    clusteringSize = columnTypes.get(columnTypes.size() - 1) instanceof ColumnToCollectionType
                                   ? columnTypes.size() - 2
                                   : columnTypes.size() - 1;
                }
            } else {
                // TODO: this is not a good test to know if it's dynamic vs static. We should also see if there is any column_metadata
                if (columnAliases.size() > 0) {
                    hasValue = true;
                    clusteringSize = columnTypes.size();
                } else {
                    hasValue = false;
                    clusteringSize = 0;
                }
            }

            for (int i = 0; i < clusteringSize; i++) {
                String cn = columnAliases.size() > i ? columnAliases.get(i) : DEFAULT_COLUMN_ALIAS + (i + 1);
                DataType dt = Codec.rawTypeToDataType(columnTypes.get(i));
                ColumnMetadata colMeta = new ColumnMetadata(tm, cn, dt, null);
                tm.columns.put(cn, colMeta);
                tm.clusteringKey.add(colMeta);
            }

            // Value alias (if present)
            if (hasValue) {
                AbstractType vt = TypeParser.parse(row.getString(VALIDATOR));
                String valueAlias = row.isNull(KEY_ALIASES) ? DEFAULT_VALUE_ALIAS : row.getString(VALUE_ALIAS);
                ColumnMetadata vm = new ColumnMetadata(tm, valueAlias, Codec.rawTypeToDataType(vt), null);
                tm.columns.put(valueAlias, vm);
            }

            ksm.add(tm);
            return tm;
        } catch (RequestValidationException e) {
            // The server will have validated the type
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the name of this table.
     *
     * @return the name of this CQL table.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the keyspace this table belong to.
     *
     * @return the keyspace metadata of the keyspace this table belong to.
     */
    public KeyspaceMetadata getKeyspace() {
        return keyspace;
    }

    /**
     * Returns metadata on a column of this table.
     *
     * @param name the name of the column to retrieve.
     * @return the metadata for the {@code name} column if it exists, or
     * {@code null} otherwise.
     */
    public ColumnMetadata getColumn(String name) {
        return columns.get(name);
    }

    // :_(
    private static ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    static List<String> fromJsonList(String json) {
        try {
            return jsonMapper.readValue(json, List.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static Map<String, String> fromJsonMap(String json) {
        try {
            return jsonMapper.readValue(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void add(ColumnMetadata column) {
        columns.put(column.getName(), column);
    }

    /**
     * Return a {@code String} containing CQL queries representing this
     * table and the index on it.
     *
     * In other words, this method returns the queries that would allow to
     * recreate the schema of this table, along with the index defined on
     * columns of this table.
     *
     * Note that the returned String is formatted to be human readable (for
     * some defintion of human readable at least).
     *
     * @return the CQL queries representing this table schema as a {code
     * String}.
     */
    public String exportAsString() {
        StringBuilder sb = new StringBuilder();

        sb.append(asCQLQuery(true)).append("\n");

        // TODO: handle indexes

        return sb.toString();
    }

    /**
     * Returns a CQL query representing this table.
     *
     * This method returns a single 'CREATE TABLE' query with the options
     * corresponding to this table definition.
     *
     * Note that the returned string will be a single line; the returned query
     * is not formatted in any way.
     *
     * @return the 'CREATE TABLE' query corresponding to this table.
     * @see #exportAsString
     */
    public String asCQLQuery() {
        return asCQLQuery(false);
    }

    private String asCQLQuery(boolean formatted) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ").append(name);
        newLine(sb, formatted);
        for (ColumnMetadata cm : columns.values())
            newLine(sb.append(spaces(4, formatted)).append(cm), formatted);

        // PK
        sb.append(spaces(4, formatted)).append("PRIMARY KEY (");
        if (partitionKey.size() == 1) {
            sb.append(partitionKey.get(0).getName());
        } else {
            sb.append("(");
            boolean first = true;
            for (ColumnMetadata cm : partitionKey) {
                if (first) first = false; else sb.append(", ");
                sb.append(cm.getName());
            }
            sb.append(")");
        }
        for (ColumnMetadata cm : clusteringKey)
            sb.append(", ").append(cm.getName());
        sb.append(")");
        newLine(sb, formatted);
        // end PK

        newLine(sb, formatted);

        // Options
        sb.append(" WITH read_repair_chance = ").append(options.readRepair);
        and(sb, formatted).append("local_read_repair_chance = ").append(options.localReadRepair);
        and(sb, formatted).append("replicate_on_write = ").append(options.replicateOnWrite);
        and(sb, formatted).append("gc_grace_seconds = ").append(options.gcGrace);
        and(sb, formatted).append("bloom_filter_fp_chance = ").append(options.bfFpChance);
        and(sb, formatted).append("caching = ").append(options.caching);
        if (options.comment != null)
            and(sb, formatted).append("comment = ").append(options.comment);

        // TODO: finish (compaction and compression)
        newLine(sb, formatted);

        return sb.toString();
    }

    private StringBuilder and(StringBuilder sb, boolean formatted) {
        return newLine(sb, formatted).append(spaces(3, formatted)).append("AND ");
    }

    private String spaces(int n, boolean formatted) {
        if (!formatted)
            return "";

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++)
            sb.append(' ');

        return sb.toString();
    }

    private StringBuilder newLine(StringBuilder sb, boolean formatted) {
        if (formatted)
            sb.append('\n');
        return sb;
    }

    public static class Options {

        private static final String COMMENT                  = "comment";
        private static final String READ_REPAIR              = "read_repair_chance";
        private static final String LOCAL_READ_REPAIR        = "local_read_repair_chance";
        private static final String REPLICATE_ON_WRITE       = "replicate_on_write";
        private static final String GC_GRACE                 = "gc_grace_seconds";
        private static final String BF_FP_CHANCE             = "bloom_filter_fp_chance";
        private static final String CACHING                  = "caching";
        private static final String COMPACTION_CLASS         = "compaction_strategy_class";
        private static final String COMPACTION_OPTIONS       = "compaction_strategy_options";
        private static final String MIN_COMPACTION_THRESHOLD = "min_compaction_threshold";
        private static final String MAX_COMPACTION_THRESHOLD = "max_compaction_threshold";
        private static final String COMPRESSION_PARAMS       = "compression_parameters";

        private static final double DEFAULT_BF_FP_CHANCE = 0.01;

        private final String comment;
        private final double readRepair;
        private final double localReadRepair;
        private final boolean replicateOnWrite;
        private final int gcGrace;
        private final double bfFpChance;
        private final String caching;
        private final Map<String, String> compaction = new HashMap<String, String>();
        private final Map<String, String> compression = new HashMap<String, String>();

        public Options(CQLRow row) {
            this.comment = row.isNull(COMMENT) ? "" : row.getString(COMMENT);
            this.readRepair = row.getDouble(READ_REPAIR);
            this.localReadRepair = row.getDouble(LOCAL_READ_REPAIR);
            this.replicateOnWrite = row.getBool(REPLICATE_ON_WRITE);
            this.gcGrace = row.getInt(GC_GRACE);
            this.bfFpChance = row.isNull(BF_FP_CHANCE) ? DEFAULT_BF_FP_CHANCE : row.getDouble(BF_FP_CHANCE);
            this.caching = row.getString(CACHING);

            // TODO: this should change (split options and handle min/max threshold in particular)
            compaction.put("class", row.getString(COMPACTION_CLASS));
            compaction.put("options", row.getString(COMPACTION_OPTIONS));

            // TODO: this should split the parameters
            compression.put("params", row.getString(COMPRESSION_PARAMS));
        }

        /**
         * The commentary set for this table.
         *
         * @return the commentary set for this table, or {@code null} if noe has been set.
         */
        public String comment() {
            return comment;
        }

        /**
         * The chance with which a read repair is triggered for this table.
         *
         * @return the read repair change set for table (in [0.0, 1.0]).
         */
        public double readRepairChance() {
            return readRepair;
        }

        /**
         * The (cluster) local read repair chance set for this table.
         *
         * @return the local read repair change set for table (in [0.0, 1.0]).
         */
        public double localReadRepairChance() {
            return localReadRepair;
        }

        /**
         * Whether replicateOnWrite is set for this table.
         *
         * This is only meaningful for tables holding counters.
         *
         * @return whether replicateOnWrite is set for this table.
         */
        public boolean replicateOnWrite() {
            return replicateOnWrite;
        }

        /**
         * The tombstone garbage collection grace time in seconds for this table.
         *
         * @return the tombstone garbage collection grace time in seconds for this table.
         */
        public int gcGraceInSeconds() {
            return gcGrace;
        }

        /**
         * The false positive chance for the bloom filter of this table.
         *
         * @return the bloom filter false positive chance for this table (in [0.0, 1.0]).
         */
        public double bloomFilterFalsePositiveChance() {
            return bfFpChance;
        }

        /**
         * The caching option for this table.
         *
         * @return the caching option for this table.
         */
        public String caching() {
            return caching;
        }

        /**
         * The compaction options for this table.
         *
         * @return a map containing the compaction options for this table.
         */
        public Map<String, String> compaction() {
            return new HashMap<String, String>(compaction);
        }

        /**
         * The compression options for this table.
         *
         * @return a map containing the compression options for this table.
         */
        public Map<String, String> compression() {
            return new HashMap<String, String>(compression);
        }
    }
}
