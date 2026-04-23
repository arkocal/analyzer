The plan file and memory files are on this machine's filesystem and won't
  transfer automatically. Here's the best approach:

  1. Copy the relevant files to the other machine (or to the same repo if
  it's checked in):
  ~/.claude/plans/joyful-frolicking-whisper.md
  ~/.claude/projects/-home-arkocal-Workspace-goblint-analyzer/memory/

  2. Start the new session with this kickoff prompt so Claude has full
  context without needing to re-derive it:

  ▎ We're reducing duplication between src/framework/control.ml and
  ▎ src/framework/fwdControl.ml in the goblint analyzer, as a prerequisite
  ▎ for adding SV-COMP verdict printing to the backwards solvers
  ▎ (fwd/bu/wbu).
  ▎
  ▎ We completed an audit. Key findings:
  ▎ - Group A (trivial): analyze_loop/analyze are identical — move to a
  ▎ shared module
  ▎ - Group B (one flag): spec_module/get_spec differ by 2 lifters — unify
  ▎ with ~fwd:bool
  ▎ - Group C (~150 lines, highest value): do_global_inits, enter_with, man,
  ▎  otherstate, do_extern_inits, print_globals, make_global_fast_xml,
  ▎ startvars construction — identical but use
  ▎ EQSys.G.spec/GVar.spec/G.create_spec which aren't in BaseGlobConstrSys.
  ▎ Adding a thin CommonSpecSys interface with those 3 accessors unlocks all
  ▎  of it.
  ▎ - Group D (blocked): solver2source_result, warn_global, RT/LT/Result,
  ▎ solve_and_postprocess, SV-COMP blocks — all blocked by LVar.t
  ▎ tuple-vs-record difference. Tackle later.
  ▎
  ▎ Next step: implement Group A+B, then C. Full plan is at
  ▎ ~/.claude/plans/joyful-frolicking-whisper.md.

  That gives the new session everything it needs without re-reading both
  700-line files.
