package org.embulk.input.bigquery

import com.google.cloud.bigquery.*
import org.embulk.config.*
import org.embulk.spi.*
import org.embulk.config.ConfigSource
import org.embulk.config.ConfigDiff
import org.embulk.spi.Schema
import org.embulk.spi.time.Timestamp
import org.embulk.spi.type.Types
import org.slf4j.Logger
import java.text.SimpleDateFormat

class BigqueryInputPlugin : InputPlugin {
    private val logger: Logger = Exec.getLogger(javaClass)

    companion object {
        private val BQ_DATETIME_FORMAT: SimpleDateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
        private val BQ_DATE_FORMAT: SimpleDateFormat = SimpleDateFormat("yyyy-MM-dd")
    }

    override fun transaction(config: ConfigSource, control: InputPlugin.Control): ConfigDiff {
        val task: PluginTask = config.loadConfig()
        val schema: Schema = schema(task)
        return resume(task.dump(), schema, 1, control)
    }

    override fun resume(taskSource: TaskSource, schema: Schema, taskCount: Int, control: InputPlugin.Control): ConfigDiff {
        val task: PluginTask = taskSource.loadTask()
        val reports: List<TaskReport> = control.run(taskSource, schema, taskCount)
        //  TODO
        return Exec.newConfigDiff()
    }

        override fun cleanup(taskSource: TaskSource, schema: Schema, taskCount: Int, successTaskReports: List<TaskReport>) {
        // TODO
    }

    override fun run(taskSource: TaskSource, schema: Schema, taskIndex: Int, output: PageOutput): TaskReport {
        val task: PluginTask = taskSource.loadTask()
        val allocator: BufferAllocator = task.getBufferAllocator()
        val pageBuilder = PageBuilder(allocator, schema, output)

        val bigQuery = BigQueryOptions.getDefaultInstance().service
        val configuration: QueryJobConfiguration = request(task)
        var response: QueryResponse = bigQuery.query(configuration)

        while (!response.jobCompleted()) {
            Thread.sleep(1000)
            response = bigQuery.getQueryResults(response.jobId)
        }

        if (response.hasErrors()) {
            for (error in response.executionErrors) {
                logger.error(error.toString())
            }
            throw RuntimeException("Response from BigQuery has Errors")
        }

        addAllRecords(response.result, pageBuilder, task)
        return Exec.newTaskReport()
    }

    override fun guess(config: ConfigSource): ConfigDiff {
        return Exec.newConfigDiff()
    }

    private fun addAllRecords(result: QueryResult, pageBuilder: PageBuilder, task: PluginTask) {
        val rowIterator: Iterator<List<FieldValue>> = result.iterateAll().iterator()
        var totalRows: Long = 0
        while (rowIterator.hasNext()) {
            val row: List<FieldValue> = rowIterator.next()
            addRecord(row, pageBuilder, task)
            totalRows++
            if (totalRows % 100000 == 0L) {
                logger.info("${totalRows} rows loaded now from BigQuery to embulk ...")
            }
        }
        pageBuilder.finish()
        logger.info("The loading from BigQuery to embulk is finished. Total ${totalRows} rows loaded.")
    }

    private fun addRecord(row: List<FieldValue>, pageBuilder: PageBuilder, task: PluginTask) {
        for ((index, value) in row.withIndex()) {
            if (value.isNull) {
                pageBuilder.setNull(index)
                continue
            }
            when (StandardSQLTypeName.valueOf(task.columns[index].type)) {
                StandardSQLTypeName.INT64 -> pageBuilder.setLong(index, value.longValue)
                StandardSQLTypeName.FLOAT64 -> pageBuilder.setDouble(index, value.doubleValue)
                StandardSQLTypeName.BOOL -> pageBuilder.setBoolean(index, value.booleanValue)
                StandardSQLTypeName.STRING -> pageBuilder.setString(index, value.stringValue)
                StandardSQLTypeName.DATETIME -> pageBuilder.setTimestamp(
                        index,
                        Timestamp.ofEpochSecond(BQ_DATETIME_FORMAT.parse(value.stringValue).time)
                )
                StandardSQLTypeName.DATE -> pageBuilder.setTimestamp(
                        index,
                        Timestamp.ofEpochSecond(BQ_DATE_FORMAT.parse(value.stringValue).time)
                )
            // TODO StandardSQLTypeName.BYTES ->
            // TODO StandardSQLTypeName.STRUCT ->
            // TODO StandardSQLTypeName.ARRAY ->
            // TODO StandardSQLTypeName.TIMESTAMP ->
            // TODO StandardSQLTypeName.TIME ->
                else -> {
                    throw ConfigException(
                            "Converting ${StandardSQLTypeName.valueOf(task.columns[index].type)} is not implemented yet"
                    )
                }
            }
        }
        pageBuilder.addRecord()
    }

    private fun schema(task: PluginTask): Schema {
        return Schema.builder().apply {
            for (column in task.columns) {

                when (StandardSQLTypeName.valueOf(column.type)) {
                    StandardSQLTypeName.INT64 -> add(column.name, Types.LONG)
                    StandardSQLTypeName.FLOAT64 -> add(column.name, Types.DOUBLE)
                    StandardSQLTypeName.BOOL -> add(column.name, Types.BOOLEAN)
                    StandardSQLTypeName.STRING -> add(column.name, Types.STRING)
                    StandardSQLTypeName.DATE -> add(column.name, Types.TIMESTAMP)
                    StandardSQLTypeName.DATETIME -> add(column.name, Types.TIMESTAMP)
                // TODO StandardSQLTypeName.BYTES ->
                // TODO StandardSQLTypeName.STRUCT ->
                // TODO StandardSQLTypeName.ARRAY ->
                // TODO StandardSQLTypeName.TIMESTAMP ->
                // TODO StandardSQLTypeName.TIME ->
                    else -> {
                        throw ConfigException(
                                "Converting ${StandardSQLTypeName.valueOf(column.type)} is not implemented yet"
                        )
                    }
                }
            }
        }.build()
    }

    private fun request(task: PluginTask): QueryJobConfiguration {
        return QueryJobConfiguration.newBuilder(task.sql).apply {
            setUseLegacySql(task.useLegacySql)
            setUseQueryCache(task.useQueryCache)
            setDryRun(task.dryRun)
            if (task.dataSet.isPresent) setDefaultDataset(task.dataSet.get())
        }.build()
    }
}
