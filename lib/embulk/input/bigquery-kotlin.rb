Embulk::JavaPlugin.register_input(
  "bigquery-kotlin", "org.embulk.input.bigquery.BigqueryInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
