package br.com.datastreambrasil.v4;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnowflakeJdbcTest {

    private static final String URL =
            "jdbc:snowflake://acme.snowflakecomputing.com?schema=LZ_IH&db=DATB200D&CLIENT_SESSION_KEEP_ALIVE=TRUE";

    @Test
    void readsDatabaseAndSchemaFromTheJdbcUrl() {
        assertEquals("DATB200D", SnowflakeJdbc.urlParam(URL, "db", "database").orElseThrow());
        assertEquals("LZ_IH", SnowflakeJdbc.urlParam(URL, "schema").orElseThrow());
    }

    @Test
    void acceptsTheDriverSynonymsAndIgnoresCase() {
        var url = "jdbc:snowflake://acme.snowflakecomputing.com?DATABASE=DATB200D&Schema=LZ_IH";

        assertEquals("DATB200D", SnowflakeJdbc.urlParam(url, "db", "database").orElseThrow());
        assertEquals("LZ_IH", SnowflakeJdbc.urlParam(url, "schema").orElseThrow());
    }

    @Test
    void decodesEscapedValues() {
        var url = "jdbc:snowflake://acme.snowflakecomputing.com?db=MY%20DB";

        assertEquals("MY DB", SnowflakeJdbc.urlParam(url, "db").orElseThrow());
    }

    @Test
    void derivesTheAccountFromTheHostLikeTheDriverDoes() {
        assertEquals("acme", SnowflakeJdbc.accountFrom(URL).orElseThrow());

        // the first host label wins, so region-qualified and PrivateLink hosts still resolve
        assertEquals("acme", SnowflakeJdbc.accountFrom(
                "jdbc:snowflake://acme.us-east-1.snowflakecomputing.com?db=X").orElseThrow());
        assertEquals("acme", SnowflakeJdbc.accountFrom(
                "jdbc:snowflake://acme.privatelink.snowflakecomputing.com?db=X").orElseThrow());

        // org-account identifiers keep their hyphen
        assertEquals("myorg-myaccount", SnowflakeJdbc.accountFrom(
                "jdbc:snowflake://myorg-myaccount.snowflakecomputing.com").orElseThrow());
    }

    @Test
    void handlesTheProductionUrlShape() {
        // region-qualified host, extra driver parameters, schema and db mixed in
        var url = "jdbc:snowflake://ab12345.us-east-1.snowflakecomputing.com"
                + "?schema=MYSCHEMA&db=MYDB&warehouse=MYWH&CLIENT_SESSION_KEEP_ALIVE=TRUE&tracing=WARNING";

        assertEquals("ab12345", SnowflakeJdbc.accountFrom(url).orElseThrow());
        assertEquals("MYDB", SnowflakeJdbc.urlParam(url, "db", "database").orElseThrow());
        assertEquals("MYSCHEMA", SnowflakeJdbc.urlParam(url, "schema").orElseThrow());
        // the endpoint keeps the full host, so the region qualifier is preserved
        assertEquals("ab12345.us-east-1.snowflakecomputing.com", SnowflakeJdbc.hostFrom(url).orElseThrow());
    }

    @Test
    void anExplicitAccountParameterWinsOverTheHost() {
        assertEquals("other", SnowflakeJdbc.accountFrom(
                "jdbc:snowflake://acme.snowflakecomputing.com?account=other").orElseThrow());
    }

    @Test
    void extractsTheHostForTheStreamingEndpoint() {
        assertEquals("acme.snowflakecomputing.com", SnowflakeJdbc.hostFrom(URL).orElseThrow());
        assertEquals("acme.privatelink.snowflakecomputing.com", SnowflakeJdbc.hostFrom(
                "jdbc:snowflake://acme.privatelink.snowflakecomputing.com:443/?db=X").orElseThrow());
        assertEquals("acme.snowflakecomputing.com", SnowflakeJdbc.hostFrom(
                "jdbc:snowflake://acme.snowflakecomputing.com").orElseThrow());
        assertTrue(SnowflakeJdbc.hostFrom("not-a-url").isEmpty());
    }

    @Test
    void reportsNothingWhenTheParameterIsMissingOrEmpty() {
        assertTrue(SnowflakeJdbc.urlParam(URL, "warehouse").isEmpty());
        assertTrue(SnowflakeJdbc.urlParam("jdbc:snowflake://acme.snowflakecomputing.com", "db").isEmpty());
        assertTrue(SnowflakeJdbc.urlParam("jdbc:snowflake://acme.snowflakecomputing.com?", "db").isEmpty());
        assertTrue(SnowflakeJdbc.urlParam("jdbc:snowflake://acme.snowflakecomputing.com?db=", "db").isEmpty());
        assertTrue(SnowflakeJdbc.urlParam(null, "db").isEmpty());
    }
}
