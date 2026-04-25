import sys

lines = sys.stdin.readlines()
lines.sort(key=lambda line: line.split()[0] if line.split() else "")
sys.stdout.writelines(lines)
