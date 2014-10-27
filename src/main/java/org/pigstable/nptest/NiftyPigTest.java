package org.pigstable.nptest;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Assert;
import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.validator.DataSetValidator;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class NiftyPigTest {
    public static final String STORAGE_PIG_CSV = "PigStorage(';')";
    public static final String STORAGE_DEFAULT = "PigStorage()";
    public static final String STORAGE_PIG_PIPE = "PigStorage('|')";
    public static final String STORAGE_PIG_AMPERSAND = "PigStorage('&')";
    public static final String STORAGE_PIG_TAB_DELIM = "PigStorage('\t')";

    private static final Logger LOG = Logger.getLogger(NiftyPigTest.class);

    /** The text of the Pig script to test with no substitution or change. */
    private final String originalTextPigScript;

    /** The list of arguments of the script. */
    private final String[] args;

    /** The list of file arguments of the script. */
    private final String[] argFiles;

    /** The list of aliases to override in the script. */
    private final Map<String, String> aliasOverrides;

    private static ThreadLocal<PigServer> pig = new ThreadLocal<PigServer>();
    private static ThreadLocal<Cluster> cluster = new ThreadLocal<Cluster>();

    private static final String EXEC_CLUSTER = "pigunit.exectype.cluster";

    /**
     * Initializes the Pig test.
     *
     * @param args The list of arguments of the script.
     * @param argFiles The list of file arguments of the script.
     * @param pigTextScript The text of the Pig script to test with no substitution or change.
     */
    @SuppressWarnings("serial")
    NiftyPigTest(String[] args, String[] argFiles, String pigTextScript) throws IOException, ParseException {
        this.originalTextPigScript = pigTextScript;
        this.args = args;
        this.argFiles = argFiles;
        this.aliasOverrides = new HashMap<String, String>() {{
            put("STORE", "");
            put("DUMP", "");
        }};

        analyzeScript();
    }


    NiftyPigTest(String[] args, String[] argFiles, String pigTextScript, Map<String, String> alias) throws IOException, ParseException {
        this.originalTextPigScript = pigTextScript;
        this.args = args;
        this.argFiles = argFiles;
        this.aliasOverrides = new HashMap<String, String>() {{
            put("STORE", "");
            put("DUMP", "");
        }};

        aliasOverrides.putAll(alias);

        analyzeScript();
    }



    public NiftyPigTest(String scriptPath,  Map<String, String> alias) throws IOException, ParseException {
       this(null, null, readFile(scriptPath), alias);
    }

    public NiftyPigTest(String scriptPath) throws IOException, ParseException {
        this(null, null, readFile(scriptPath));
    }

    public NiftyPigTest(String[] script) throws IOException, ParseException {
        this(null, null, StringUtils.join(script, "\n"));
    }

    public NiftyPigTest(String scriptPath, String[] args) throws IOException, ParseException {
        this(args, null, readFile(scriptPath));
    }

    public NiftyPigTest(String[] script, String[] args) throws IOException, ParseException {
        this(args, null, StringUtils.join(script, "\n"));
    }

    public NiftyPigTest(String[] script, String[] args, String[] argsFile) throws IOException, ParseException {
        this(args, argsFile, StringUtils.join(script, "\n"));
    }

    public NiftyPigTest(String scriptPath, String[] argFiles, Map<String, String> alias) throws IOException, ParseException {
        this( null, argFiles, readFile(scriptPath), alias);
    }

    public NiftyPigTest(String scriptPath, String[] args, String[] argFiles) throws IOException, ParseException {
        this(args, argFiles, readFile(scriptPath));
    }

    /**
     * Connects and starts if needed the PigServer.
     *
     * @return Reference to the Cluster in ThreadLocal.
     * @throws org.apache.pig.backend.executionengine.ExecException If the PigServer can't be started.
     */
    public static Cluster getCluster() throws ExecException {
        if (cluster.get() == null) {
            if (System.getProperties().containsKey(EXEC_CLUSTER)) {
                LOG.info("Using cluster mode");
                pig.set(new PigServer(ExecType.MAPREDUCE));
            } else {
                LOG.info("Using default local mode");
                pig.set(new PigServer(ExecType.LOCAL));
            }

            cluster.set(new Cluster(pig.get().getPigContext()));
        }

        return cluster.get();
    }

    /**
     * Return the PigServer.
     *
     * @return Reference to the PigServer in ThreadLocal.
     */
    public static PigServer getPigServer() {
        return pig.get();
    }

    /**
     * Executes the Pig script with its current overrides.
     *
     * @throws java.io.IOException If a temp file containing the pig script could not be created.
     * @throws org.apache.pig.tools.parameters.ParseException The pig script could not have all its variables substituted.
     */
    public void execute() throws IOException, ParseException {
        getCluster();

        BufferedReader pigIStream = new BufferedReader(new StringReader(this.originalTextPigScript));
        StringWriter pigOStream = new StringWriter();

        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        ps.genSubstitutedFile(pigIStream, pigOStream, args, argFiles);

        String substitutedPig = pigOStream.toString();
        LOG.info(substitutedPig);

        File f = File.createTempFile("tmp", "pigunit");
        PrintWriter pw = new PrintWriter(f);
        pw.println(substitutedPig);
        pw.close();

        String pigSubstitutedFile = f.getCanonicalPath();

        PigServer pigServer = getPigServer();
        pigServer.registerScript(pigSubstitutedFile, aliasOverrides);
    }

    /**
     * Analyze the given pig script.
     *
     * @throws java.io.IOException If a temp file containing the pig script could not be created.
     * @throws org.apache.pig.tools.parameters.ParseException The pig script could not have all its variables substituted.
     */
    protected void analyzeScript() throws IOException, ParseException {
        getCluster();

        // -- Clean the pig script from all DUMP, ILLUSTRATE and EXPLAIN statements. We only want to analyze it, not
        // -- execute it.
        String cleanedScript = originalTextPigScript;
        cleanedScript = cleanedScript.replaceAll("DUMP .*;", "");
        cleanedScript = cleanedScript.replaceAll("ILLUSTRATE .*;", "");
        cleanedScript = cleanedScript.replaceAll("EXPLAIN .*;", "");

        BufferedReader pigIStream = new BufferedReader(new StringReader(cleanedScript));

        File f = File.createTempFile("tmp", "pigunit");
        PrintWriter pw = new PrintWriter(f);

        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        ps.genSubstitutedFile(pigIStream, pw, args, argFiles);
        pw.close();

        String pigSubstitutedFile = f.getCanonicalPath();

        LOG.info(pigSubstitutedFile);

        getPigServer().registerScript(pigSubstitutedFile, aliasOverrides);
    }

    /**
     * Gets an iterator on the content of one alias of the script.
     *
     * <p>For now use a giant String in order to display all the differences in one time. It might not
     * work with giant expected output.
     * @throws java.io.IOException If the Pig script could not be executed correctly.
     */
    public Iterator<Tuple> getAlias(String alias) throws IOException {
        return getPigServer().openIterator(alias);
    }

    /**
     * Gets an iterator on the content of the latest STORE alias of the script.
     *
     * @throws java.io.IOException If the Pig script could not be executed correctly.
     */
    public Iterator<Tuple> getAlias() throws IOException {
        String alias = aliasOverrides.get("LAST_STORE_ALIAS");

        return getAlias(alias);
    }

    /**
     * Replaces the query of an aliases by another query.
     *
     * <p>For example:
     *
     * <pre>
     * B = FILTER A BY count > 5;
     * overridden with:
     * &lt;B, B = FILTER A BY name == 'Pig';&gt;
     * becomes
     * B = FILTER A BY name == 'Pig';
     * </pre>
     *
     * @param alias The alias to override.
     * @param query The new value of the alias.
     */
    public void override(String alias, String query) {
        aliasOverrides.put(alias, query);
    }

    public void unoverride(String alias) {
        aliasOverrides.remove(alias);
    }

    protected void assertEquals(String expected, String current) {
        Assert.assertEquals(expected, current);
    }

    private static String readFile(String path) throws IOException {
        return readFile(new File(path));
    }

    private static String readFile(File file) throws IOException {
        FileInputStream stream = new FileInputStream(file);
        try {
            FileChannel fc = stream.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            return Charset.defaultCharset().decode(bb).toString();
        }
        finally {
            stream.close();
        }
    }

    public void input(String alias, String[] data) throws IOException, ParseException {
        input(alias, data, STORAGE_DEFAULT);
    }

    public void input(String alias, String[] data, String storage) throws IOException, ParseException {
        analyzeScript();

        StringBuilder sb = new StringBuilder();
        Schema.stringifySchema(sb, getPigServer().dumpSchema(alias), DataType.TUPLE) ;

        final String destination = FileLocalizer.getTemporaryPath(getPigServer().getPigContext()).toString();
        getCluster().copyFromLocalFile(data, destination, true);
        LOG.warn(String.format("Replaced %s with the given data (stored in file %s)", alias, destination));
        override(alias, String.format("%s = LOAD '%s' USING %s AS %s;", alias, destination, storage, sb.toString()));
    }

    public void input(String alias,
                      String[] data,
                      String storage,
                      String schema) throws IOException, ParseException {
        analyzeScript();

        final String destination = FileLocalizer.getTemporaryPath(getPigServer().getPigContext()).toString();
        getCluster().copyFromLocalFile(data, destination, true);
        LOG.warn(String.format("Replaced %s with the given data (stored in file %s)", alias, destination));
        override(alias, String.format("%s = LOAD '%s' USING %s AS %s;", alias, destination, storage, schema));
    }

    public DataSetReport validate(DataSetValidator.Builder validatorBuilder) throws IOException {
        DataSetValidator validator = validatorBuilder.result();

        return validator.validate(getAlias(validator.getName()));
    }
}
