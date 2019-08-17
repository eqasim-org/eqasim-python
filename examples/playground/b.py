def configure(require):
    require.config("config_b", default = "uvw")
    require.stage("playground.c")

def verify(context):
    return "v1"

def execute(context):
    with open("%s/output.txt" % context.cache_path(), "w+") as f:
        f.write("hallo")

    print("Stage B")
    print("  " + context.stage("playground.c"))
