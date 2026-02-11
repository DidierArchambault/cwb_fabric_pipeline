# this file contains the help table for the CLI


HELP_TABLE = r"""

    |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    ||                                                                                 ||
    ||  Commands                                                                       ||
    ||  --------                                                                       ||
    ||  bronze run      Run bronze layer                                               ||
    ||  silver run      Run silver layer                                               ||
    ||  gold run        Run gold layer                                                 ||
    ||                                                                                 ||
    ||  Common options (all runs)                                                      ||
    ||  -----------------------                                                        ||
    ||  --env, -e             ENV selector: prod|dev|qa (default: dev)                 ||
    ||                                                                                 ||
    ||  Bronze options (bronze run)                                                    ||
    ||  -----------------------                                                        ||
    ||  --xcenter, -xc        X-Center selector: AB|BC|CC|PC (default: CC)             ||
    ||  --work, -w            Work selector: ingest_data|add_technicals|export_to_delta||
    ||                        Default to all                                           ||
    ||                                                                                 ||
    ||  Silver options (silver run)                                                    ||
    ||  -----------------------                                                        ||
    ||  --work, -w            Work selector: all|run_one_script (default: all)         ||
    ||                                                                                 ||
    ||  Gold options (gold run)                                                        ||
    ||  -----------------------                                                        ||
    ||  --semantic             Semantic scope: facts|dims|bridge|all (default: all)    ||
    ||  --work, -w             Work selector: all|run_one_script (default: all)        ||
    ||  --xcenter, -xc         X-Center selector: AB|BC|CC|PC (default: CC)            ||
    ||  --semantic_one, -so    Single semantic group:                                  ||
    ||                        fact_group_1|fact_group_2|fact_group_3|                  ||
    ||                        dim_group_1|dim_group_2 (default: all)                   ||
    ||                                                                                 ||
    ||  Examples                                                                       ||
    ||  --------                                                                       ||
    ||  SparkJobRunner bronze run                                                      ||
    ||  SparkJobRunner bronze run --work ingest_data -xc CC                            ||
    ||  SparkJobRunner silver run --work all                                           ||
    ||  SparkJobRunner silver run --work run_one_script                                ||
    ||  SparkJobRunner gold run --semantic facts                                       ||
    ||  SparkJobRunner gold run --semantic facts --semantic_one fact_group_1           ||
    ||                                                                                 ||
    |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


"""
