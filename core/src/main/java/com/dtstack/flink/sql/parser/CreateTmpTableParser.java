package com.dtstack.flink.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

public class CreateTmpTableParser implements IParser {

    //select table tableName as select
    private static final String PATTERN_STR = "(?i)create\\s+table\\s+([^\\s]+)\\s+as\\s+select\\s+(.*)";

    private static final String WITHOUT_STR = "(?i)^\\screate\\s+table\\s+(\\S+)\\s*\\((.+)\\)$";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    private static final Pattern PATTERN2 = Pattern.compile(WITHOUT_STR);

    public static CreateTmpTableParser newInstance(){
        return new CreateTmpTableParser();
    }

    @Override
    public boolean verify(String sql) {
        if (Pattern.compile(WITHOUT_STR).matcher(sql).find()){
            return true;
        }
        return PATTERN.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        if (PATTERN.matcher(sql).find()){
            Matcher matcher = PATTERN.matcher(sql);
            String tableName = null;
            String selectSql = null;
            if(matcher.find()) {
                tableName = matcher.group(1).toUpperCase();
                selectSql = "select " + matcher.group(2);
            }

            SqlParser sqlParser = SqlParser.create(selectSql);
            SqlNode sqlNode = null;
            try {
                sqlNode = sqlParser.parseStmt();
            } catch (SqlParseException e) {
                throw new RuntimeException("", e);
            }

            CreateTmpTableParser.SqlParserResult sqlParseResult = new CreateTmpTableParser.SqlParserResult();
            parseNode(sqlNode, sqlParseResult);

            sqlParseResult.setTableName(tableName);
            sqlParseResult.setExecSql(selectSql.toUpperCase());
            sqlTree.addTmpSql(sqlParseResult);
            sqlTree.addTmplTableInfo(tableName, sqlParseResult);
        } else {
            if (PATTERN2.matcher(sql).find())
            {
                Matcher matcher = PATTERN2.matcher(sql);
                String tableName = null;
                String fieldsInfoStr = null;
                if (matcher.find()){
                    tableName = matcher.group(1).toUpperCase();
                    fieldsInfoStr = matcher.group(2);
                }
                CreateTmpTableParser.SqlParserResult sqlParseResult = new CreateTmpTableParser.SqlParserResult();
                sqlParseResult.setFieldsInfoStr(fieldsInfoStr);
                sqlParseResult.setTableName(tableName);
                sqlTree.addTmplTableInfo(tableName, sqlParseResult);
            }

        }

    }

    private static void parseNode(SqlNode sqlNode, CreateTmpTableParser.SqlParserResult sqlParseResult){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
                if(sqlFrom.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(sqlFrom.toString());
                }else{
                    parseNode(sqlFrom, sqlParseResult);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin)sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin)sqlNode).getRight();

                if(leftNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(leftNode.toString());
                }else{
                    parseNode(leftNode, sqlParseResult);
                }

                if(rightNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(rightNode.toString());
                }else{
                    parseNode(rightNode, sqlParseResult);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall)sqlNode).getOperands()[0];
                if(identifierNode.getKind() != IDENTIFIER){
                    parseNode(identifierNode, sqlParseResult);
                }else {
                    sqlParseResult.addSourceTable(identifierNode.toString());
                }
                break;
            default:
                //do nothing
                break;
        }
    }

    public static class SqlParserResult {
        private String tableName;

        private String fieldsInfoStr;

        private String execSql;

        private List<String> sourceTableList = Lists.newArrayList();

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getExecSql() {
            return execSql;
        }

        public void setExecSql(String execSql) {
            this.execSql = execSql;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public void addSourceTable(String sourceTable){
            sourceTableList.add(sourceTable);
        }

        public List<String> getSourceTableList() {
            return sourceTableList;
        }


    }
}
