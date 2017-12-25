package org.embulk.input.bigquery

import org.embulk.config.Config
import org.embulk.config.Task

interface Column : Task {
    @get:Config("name")
    val name: String

    @get:Config("type")
    val type: String
}