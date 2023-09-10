package com.vtw.calcite;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;

import javax.sql.DataSource;
import java.sql.DriverManager;
import java.util.List;

public class QueryToNode {

    public static void main(String[] args) throws Exception {
        /**
         PostgreSQL: org.postgresql.Driver
         MySQL: com.mysql.jdbc.Driver
         Oracle: oracle.jdbc.OracleDriver
         Tibero: com.tmax.tibero.jdbc.TbDriver

         [Oracle]
         jdbc:oracle:thin:@180.210.82.175:1521:XE
         vtw/30539w

         [PostgreSQL]
         jdbc:postgresql://180.210.83.49:6543/portal
         vtw/vtw123#

         [Mysql]
         root / Vtw1234#
         jdbc:mysql://180.210.80.174:3306/portal
         vtw / Vtw1234#

         [Tibero]
         jdbc:tibero:thin:@180.210.80.61:8639:portal
         vtw / 30539ww
         */
        String product = "PostgreSQL";
        String driver = "org.postgresql.Driver";
        String url = "jdbc:postgresql://180.210.83.49:6543/portal";
        String username = "vtw";
        String password = "vtw123#";

        String schema = "public";
        String query = """
                SELECT
                    w.id,
                    w.tmef,
                    w.title,
                    w.regname,
                    i.clsfc,
                    i.dangerlevel
                FROM
                    public.keco_heatwave_wthrwrninfo as w
                        INNER JOIN public.keco_heatwave_impactinfo as i
                                   ON (i.regid = w.regid AND i.tmef = w.tmef)
                """;

        CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:").unwrap(CalciteConnection.class);
        DataSource dataSource = JdbcSchema.dataSource(url, driver, username, password);

        try (connection) {
            SchemaPlus rootSchema = connection.getRootSchema();
            rootSchema.add(schema, JdbcSchema.create(rootSchema, schema, dataSource, null, schema));

            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .parserConfig(SqlParser.config().withCaseSensitive(true))
                    .parserConfig(SqlParser.config().withQuotedCasing(Casing.UNCHANGED))
                    .parserConfig(SqlParser.config().withUnquotedCasing(Casing.UNCHANGED))
                    .defaultSchema(rootSchema)
                    .build();

            Planner planner = Frameworks.getPlanner(config);
            SqlNode sqlNode = planner.parse(query);
            SqlNode validateNode = planner.validate(sqlNode);

            RelBuilder rb = RelBuilder.create(config);

            RelNode bodyNode = planner.rel(validateNode).project();
            rb.push(bodyNode);

            rb = rb
                    .filter(
                            rb.call(SqlStdOperatorTable.LIKE, rb.field("title"), new RexDynamicParam(rb.field("title").getType(), 0))
                    );

            bodyNode = rb.build();

            SqlDialect sqlDialect = null;
            if (product.equals("PostgreSQL")) {
                sqlDialect = new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT);
            } else if (product.equals("Oracle") || product.equals("Tibero")) {
                sqlDialect = new OracleSqlDialect(OracleSqlDialect.DEFAULT_CONTEXT);
            } else if (product.equals("MySql")) {
                sqlDialect = new OracleSqlDialect(MysqlSqlDialect.DEFAULT_CONTEXT);
            }

            RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
            String result = converter.visitRoot(bodyNode).asQueryOrValues().toSqlString(sqlDialect).getSql();

            System.out.println();
            System.out.println("===== Generated SQL query =====");
            System.out.println(result);
            System.out.println("===============================");
        }
    }

    private static String recursive(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) node;
            List<String> names = sqlIdentifier.names;
            String name = names.get(names.size() - 1);
            return name;
        } else if (node instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) node;
            List<SqlNode> operandList = sqlBasicCall.getOperandList();
            return recursive(operandList.get(operandList.size() - 1));
        }
        throw new RuntimeException("Invalid SqlNode: " + node);
    }

}
