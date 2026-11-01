INSERT INTO gcra_hot_rounded.preflight_decisions SELECT allowed FROM gcra_hot_rounded.charge('check', 'concurrent', 60000000, 8);
