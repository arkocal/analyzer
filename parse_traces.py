import sys
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from rich.console import Console
from rich.padding import Padding
from rich.panel import Panel
from rich.text import Text

console = Console()

# ── parsing ──────────────────────────────────────────────────────────────────

def parse_traces(filename):
    traces = []
    current = None
    with open(filename) as f:
        for line in f:
            if line.startswith("%%%"):
                if current is not None:
                    traces.append(current.rstrip())
                current = line
            elif line.startswith("["):
                if current is not None:
                    traces.append(current.rstrip())
                    current = None
            else:
                if current is not None:
                    current += line
    if current is not None:
        traces.append(current.rstrip())
    return traces


_PREFIX = re.compile(r"^%%% prec: \[#(\d+) d=(\d+)\] (.*)$", re.DOTALL)

def strip_prefix(trace):
    m = _PREFIX.match(trace)
    if not m:
        return None, None, trace
    return int(m.group(1)), int(m.group(2)), m.group(3)


@dataclass
class UpdateEvent:
    id: int
    depth: int = 0
    from_node: str = ""
    to_node: str = ""
    value_before: str = ""
    contribution_before: str = ""
    update_value: str = ""
    path: str = ""
    contribution_after: str = ""
    value_after: Optional[str] = None
    value_unchanged: bool = False
    contribution_unchanged: bool = False


def parse_value(rest):
    return rest.split(": ", 1)[1] if ": " in rest else rest


def build_events(traces):
    groups = defaultdict(list)
    depths = {}
    for trace in traces:
        event_id, depth, rest = strip_prefix(trace)
        if event_id is not None:
            groups[event_id].append(rest)
            if event_id not in depths:
                depths[event_id] = depth

    events = {}
    for event_id, lines in sorted(groups.items()):
        ev = UpdateEvent(id=event_id, depth=depths.get(event_id, 0))
        for line in lines:
            if line.startswith("local update:"):
                m = re.match(r"local update: (\S+) -> (\S+)", line)
                if m:
                    ev.from_node, ev.to_node = m.group(1), m.group(2)
            elif line.startswith("value before:"):
                ev.value_before = parse_value(line)
            elif line.startswith("contribution from") and "before:" in line:
                ev.contribution_before = parse_value(line)
            elif line.startswith("update value:"):
                ev.update_value = parse_value(line)
            elif line.startswith("path:"):
                ev.path = parse_value(line)
            elif line.startswith("contribution from") and "after:" in line:
                ev.contribution_after = parse_value(line)
            elif line.startswith("value after:"):
                ev.value_after = parse_value(line)
            elif line.startswith("value unchanged:"):
                ev.value_unchanged = True
                ev.value_after = parse_value(line)
            elif line.startswith("contribution unchanged"):
                ev.contribution_unchanged = True
        events[event_id] = ev
    return events


# ── value simplification ─────────────────────────────────────────────────────

def extract_braces(s: str, start: int) -> Optional[str]:
    """Extract the balanced {…} block beginning at index start."""
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                return s[start:i + 1]
    return None


def extract_base(value: str) -> str:
    """Simplify a value string to its most informative part.

    - If base:( is present, extract the first {…} block inside it.
    - If that block contains Local {…}, return only the interior of Local,
      with newlines replaced by ", " and whitespace stripped.
    """
    idx = value.find("base:(")
    if idx == -1:
        return value.strip()
    start = value.find("{", idx + len("base:("))
    if start == -1:
        return value.strip()
    base_block = extract_braces(value, start)
    if base_block is None:
        return value.strip()

    local_idx = base_block.find("Local {")
    if local_idx == -1:
        return base_block.strip()
    local_start = base_block.find("{", local_idx + len("Local"))
    local_block = extract_braces(base_block, local_start)
    if local_block is None:
        return base_block.strip()

    interior = local_block[1:-1]  # strip outer { }
    parts = [p.strip() for p in interior.split("\n") if p.strip()]
    return ", ".join(parts)


# ── colors ───────────────────────────────────────────────────────────────────

PATH_STYLE = {
    "overwrite":           "bold blue",
    "widen":               "bold red",
    "narrow":              "bold green",
    "update":              "bold yellow",
    "widen(incomparable)": "bold dark_orange",
}

def style_for(path: str) -> str:
    return PATH_STYLE.get(path, "bold white")


# ── display ───────────────────────────────────────────────────────────────────

def print_event(ev: UpdateEvent):
    style = style_for(ev.path)
    left_pad = 2 * (ev.depth - 1)

    header = Text()
    header.append(f"#{ev.id}", style=f"bold {style}")
    header.append(f"  d={ev.depth}", style="dim")
    header.append(f"  {ev.from_node}", style="cyan")
    header.append(" → ", style="white")
    header.append(f"{ev.to_node}", style="cyan")
    header.append(f"  [{ev.path}]", style=style)

    body = Text()
    body.append("value before:        ", style="dim")
    body.append(extract_base(ev.value_before) + "\n")

    body.append("contrib before:      ", style="dim")
    body.append(extract_base(ev.contribution_before) + "\n")

    body.append("update value:        ", style="dim")
    body.append(extract_base(ev.update_value) + "\n")

    body.append("contrib after:       ", style="dim")
    if ev.contribution_unchanged:
        body.append("(unchanged)\n", style="dim italic")
    else:
        body.append(extract_base(ev.contribution_after) + "\n")

    body.append("value after:         ", style="dim")
    if ev.value_after is None:
        body.append("(not set)\n", style="dim italic")
    elif ev.value_unchanged:
        body.append(f"(unchanged) {extract_base(ev.value_after)}\n", style="dim italic")
    else:
        body.append(extract_base(ev.value_after) + "\n")

    panel = Panel(
        Text.assemble(header, "\n", body),
        border_style=style,
        expand=False,
        padding=(0, 1),
    )
    console.print(Padding(panel, pad=(0, 0, 0, left_pad)))


if __name__ == "__main__":
    traces = parse_traces(sys.argv[1])
    events = build_events(traces)
    for ev in events.values():
        print_event(ev)
