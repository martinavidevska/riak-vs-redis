import subprocess
import re
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import redis

scripts = [
    "insert_redis.py",
    "load_to_redis.py",
    "read_redis.py",
    "redis_performance.py",
    "redis_queries.py"
]

def run_script(script):
    print(f"Running script: {script}...")
    try:
        result = subprocess.run(
            ["python", script],
            capture_output=True,
            text=True,
            timeout=300
        )
        output = result.stdout + result.stderr
        print(output)
        return output
    except Exception as e:
        print(f"Error {script}: {e}")
        return ""

def parse_time(output):
    times = re.findall(r"(\d+\.\d+)\s*(seconds|sec|секунди|сек)", output, re.IGNORECASE)
    if times:
        return float(times[0][0])
    return None

def main():
    results = {}
    for script in scripts:
        output = run_script(script)
        t = parse_time(output)
        results[script] = t

    # Плотираме
    names = list(results.keys())
    times = [results[name] if results[name] is not None else 0 for name in names]

    plt.figure(figsize=(10,6))
    plt.barh(names, times, color='deepskyblue')
    plt.xlabel("Време во секунди")
    plt.title("Redis Скрипти: Време на извршување")
    for i, v in enumerate(times):
        plt.text(v + 0.01, i, f"{v:.4f}s", va='center')
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()