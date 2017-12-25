package org.embulk.input.bigquery

import com.google.common.base.Optional
import org.embulk.config.Config
import org.embulk.config.ConfigDefault
import org.embulk.config.ConfigInject
import org.embulk.config.Task
import org.embulk.spi.BufferAllocator

interface PluginTask : Task {
    @get:Config("sql")
    val sql: String

    @get:Config("data_set")
    @get:ConfigDefault("null")
    val dataSet: Optional<String>

    @get:Config("use_query_cache")
    @get:ConfigDefault("false")
    val useQueryCache: Boolean

    @get:Config("use_legacy_sql")
    @get:ConfigDefault("false")
    val useLegacySql: Boolean

    @get:Config("dry_run")
    @get:ConfigDefault("false")
    val dryRun: Boolean

    @get:Config("columns")
    @get:ConfigDefault("{}")
    val columns: List<Column>

    @ConfigInject
    fun getBufferAllocator(): BufferAllocator
}
