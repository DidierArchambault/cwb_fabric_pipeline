# this file contains the help table for the CLI

HELP_TABLE = r"""

    |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    ||                                                                                 ||
    ||  Commands                                                                       ||
    ||  --------                                                                       ||
    ||  bronze run      Run bronze layer                                               ||
    ||                                                                                 ||
    ||  Common options (all runs)                                                      ||
    ||  -----------------------                                                        ||
    ||  --env, -e             Environment selector: prod|dev|qa (default: dev)         ||
    ||  --config, -c          Config source: local|fabric (default: local)             ||
    ||                        - local  : uses packaged defaults                        ||
    ||                        - fabric : uses BRONZE_CONFIG_PATH env var               ||
    ||                                                                                 ||
    ||  Bronze options (bronze run)                                                    ||
    ||  -----------------------                                                        ||
    ||  --xcenter, -xc        X-Center selector: AB|BC|CC|PC (default: CC)             ||
    ||  --work, -w            Step selector:                                           ||
    ||                        all|ingest_data|add_technicals|export_to_delta           ||
    ||                        (default: all)                                           ||
    ||                                                                                 ||
    ||  Examples                                                                       ||
    ||  --------                                                                       ||
    ||  bronze_layer bronze run                                                        ||
    ||  bronze_layer bronze run --work ingest_data -xc CC                              ||
    ||  bronze_layer bronze run -e qa -c fabric --work export_to_delta                 ||
    ||                                                                                 ||
    |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


"""
