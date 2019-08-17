import time
import numpy as np

def configure(require):
    require.stage("playground.b")

def execute(context):
    print("Stage A")

    with context.progress("Stage A", total = 100, interval = 0.01) as progress:
        for i in range(100):
            progress.update()
            time.sleep(np.random.random() * 0.01)

    with open("%s/output.txt" % context.cache_path("playground.b")) as f:
        print("  " + f.read())
