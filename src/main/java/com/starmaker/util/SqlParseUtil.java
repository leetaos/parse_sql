package com.starmaker.util;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;


public class SqlParseUtil {
    public static List<String> getTableNamesFromSql(String sql) {

        // 数据源 : hive
        DbType dbType = JdbcConstants.HIVE;
        List<String> tableNameList = new ArrayList<>();
        try {

            // 格式化sql(关键字格式化为大写)
            String sqlFormat = SQLUtils.format(sql, dbType);

            // 解析多条sql
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sqlFormat, dbType);
            if (CollectionUtils.isEmpty(stmtList)) {
                return tableNameList;
            }

            //遍历sqlStatement，获取每个sqlStatement操作的table_name
            for (SQLStatement sqlStatement : stmtList) {

                //获取HiveSchemaStatVisitor
                HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
                sqlStatement.accept(visitor);

                //获取表名
                Map<TableStat.Name, TableStat> tables = visitor.getTables();


                for (Map.Entry<TableStat.Name, TableStat> entry : tables.entrySet()) {
                    TableStat.Name tabName = entry.getKey();
                    TableStat tabOperation = entry.getValue();
                    // 查询操作记录表名
                    if ("Select".equalsIgnoreCase(tabOperation.toString())) {
                        String name = tabName.getName();

                        // 校验是否为空
                        if (StringUtils.isNotBlank(name)) {

                            tableNameList.add(name);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return tableNameList;
        }
        return tableNameList;
    }

    public static List<String> getColNamesFromSql(String sql) {

        // 数据源 : hive
        DbType dbType = JdbcConstants.HIVE;
        List<String> colNameList = new ArrayList<>();
        try {

            // 格式化sql(关键字格式化为大写)
            String sqlFormat = SQLUtils.format(sql, dbType);

            // 解析多条sql
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sqlFormat, dbType);

            if (CollectionUtils.isEmpty(stmtList)) {
                return colNameList;
            }

            //遍历sqlStatement，获取每个sqlStatement操作的字段
            for (SQLStatement sqlStatement : stmtList) {

                //获取HiveSchemaStatVisitor
                HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
                sqlStatement.accept(visitor);

                //获取查询字段
                Collection<TableStat.Column> columns = visitor.getColumns();

                for (TableStat.Column column : columns) {

                    // 无需只计算select字段,因为join,insert字段也是高频字段
//                    System.out.println(column.isSelect());
                    String tb = column.getTable();
                    String nm = column.getName();

                    // 排除掉dt字段和UNKNOWN表
                    if ("dt".equals(nm) || "UNKNOWN".equals(tb)) {
                        continue;
                    }
                    colNameList.add(tb + " " + nm);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return colNameList;
        }
        return colNameList;
    }


    public static List<String> getConditionsFromSql(String sql) {

        // 数据源 : hive
        DbType dbType = JdbcConstants.HIVE;
        List<String> conditionList = new ArrayList<>();
        try {

            // 格式化sql(关键字格式化为大写)
            String sqlFormat = SQLUtils.format(sql, dbType);

            // 解析多条sql
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sqlFormat, dbType);

            if (CollectionUtils.isEmpty(stmtList)) {
                return conditionList;
            }

            //遍历sqlStatement，获取每个sqlStatement操作的字段
            for (SQLStatement sqlStatement : stmtList) {

                //获取HiveSchemaStatVisitor
                HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
                sqlStatement.accept(visitor);

                //获取查询条件
                List<TableStat.Condition> conditions = visitor.getConditions();
                for (TableStat.Condition condition : conditions) {
                    conditionList.add(condition.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return conditionList;
        }
        return conditionList;
    }



    public static List<String> getConditionsFromSql2(String sql) {

        // 数据源 : hive
        DbType dbType = JdbcConstants.HIVE;
        List<String> conditionList = new ArrayList<>();
        try {

            // 格式化sql(关键字格式化为大写)
            String sqlFormat = SQLUtils.format(sql, dbType);


            // 解析多条sql
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sqlFormat, dbType);

            if (CollectionUtils.isEmpty(stmtList)) {
                return conditionList;
            }

            //遍历sqlStatement，获取每个sqlStatement操作的字段
            for (SQLStatement sqlStatement : stmtList) {

                //获取HiveSchemaStatVisitor
                HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
                sqlStatement.accept(visitor);

                //获取查询条件
                List<TableStat.Condition> conditions = visitor.getConditions();
                for (TableStat.Condition condition : conditions) {
                    System.out.println(condition.getColumn());
                    System.out.println(condition.getOperator());
                    System.out.println(condition.getValues());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return conditionList;
        }
        return conditionList;
    }

    public static void main(String[] args) {
        String sql = "-- okokoik\n" +
                "with tb_new_dev as\n" +
                "         (\n" +
                "             select device_new_all.device_id device_id\n" +
                "             from (\n" +
                "                      select device_id -- okokok\n" +
                "                      from base_table.device_attr_install\n" +
                "                      where dt = '${dt}'\n" +
                "                  ) device_new_all\n" +
                "                      left outer join\n" +
                "                  (\n" +
                "                      select device_id\n" +
                "                      from (\n" +
                "                               select user_id\n" +
                "                               from starmaker_emr.user_garbage_account\n" +
                "                               where dt = from_unixtime(unix_timestamp('${dt}', 'yyyyMMdd') + 86400 * 29, 'yyyyMMdd')\n" +
                "                                 and level >= 80\n" +
                "                               group by user_id\n" +
                "                           ) garbage_user\n" +
                "                               join\n" +
                "                           (\n" +
                "                               select user_id,\n" +
                "                                      device_id\n" +
                "                               from starmaker_emr.user_device\n" +
                "                               where dt = from_unixtime(unix_timestamp('${dt}', 'yyyyMMdd') + 86400 * 29, 'yyyyMMdd')\n" +
                "                           ) user_device\n" +
                "                           on garbage_user.user_id = user_device.user_id\n" +
                "                      group by device_id\n" +
                "                  ) garbage_device\n" +
                "                  on device_new_all.device_id = garbage_device.device_id\n" +
                "             where garbage_device.device_id is null\n" +
                "         ),\n" +
                "     tb_active as\n" +
                "         (\n" +
                "             select device_id,\n" +
                "                    count(dt) ct_active,\n" +
                "                    0         ct_play,\n" +
                "                    0         ct_search,\n" +
                "                    0         ct_sing,\n" +
                "                    0         ct_like,\n" +
                "                    0         ct_register\n" +
                "             from base_table.daily_active_device_max_attr_src_mobile\n" +
                "             where dt >= '${dt}'\n" +
                "               and dt <= from_unixtime(unix_timestamp('${dt}', 'yyyyMMdd') + 86400 * 29, 'yyyyMMdd')\n" +
                "             group by device_id\n" +
                "         )\n" +
                "select device_id,\n" +
                "       sum(ct_active)                                                              ct_active,\n" +
                "       sum(ct_play)                                                                ct_play,\n" +
                "       sum(ct_search)                                                              ct_search,\n" +
                "       sum(ct_sing)                                                                ct_sing,\n" +
                "       sum(ct_like)                                                                ct_like,\n" +
                "       sum(ct_register)                                                            ct_register,\n" +
                "       '${dt}'                                                                     start_dt,\n" +
                "       from_unixtime(unix_timestamp('${dt}', 'yyyyMMdd') + 86400 * 29, 'yyyyMMdd') end_dt\n" +
                "from (\n" +
                "         select tb_active.device_id device_id, ct_active, ct_play, ct_search, ct_sing, ct_like, ct_register\n" +
                "         from tb_active\n" +
                "                  left semi\n" +
                "                  join tb_new_dev on tb_active.device_id = tb_new_dev.device_id\n" +
                "         union all\n" +
                "         select tb_page.device_id                                device_id,\n" +
                "                0                                                ct_active,\n" +
                "                sum(if(type = 'play', 1, 0))                     ct_play,\n" +
                "                sum(if(type = 'click' and obj = 'search', 1, 0)) ct_search,\n" +
                "                sum(if(type = 'click' and obj = 'sing', 1, 0))   ct_sing,\n" +
                "                sum(if(type = 'click' and obj = 'like', 1, 0))   ct_like,\n" +
                "                sum(if(type = 'register', 1, 0))                 ct_register\n" +
                "         from (\n" +
                "                  select device_id,\n" +
                "                         type,\n" +
                "                         obj\n" +
                "                  from event_tracks.format_line_tracks_page\n" +
                "                  where (dt = '${dt}')\n" +
                "                    and ((type = 'play' and obj in ('finish', 'player_finished'))\n" +
                "                      or (type = 'click' and obj in ('search', 'sing', 'like'))\n" +
                "                      or (type = 'register' and result = 'success' and page <> 'push'))\n" +
                "              ) tb_page\n" +
                "                  left semi\n" +
                "                  join tb_new_dev on tb_page.device_id = tb_new_dev.device_id\n" +
                "         group by tb_page.device_id\n" +
                "     ) tmp_union\n" +
                "group by device_id;-- okok";

        DbType type = JdbcConstants.HIVE;

        System.out.println(SQLUtils.format(sql, type));


    }

}
