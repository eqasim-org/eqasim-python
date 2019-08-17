def configure(require):
    require.config("config_b")

def verify(context):
    return "v3"

def execute(context):
    print("Stage C")

    return "blabla"
