INSERT INTO gcra_hot_baseline.preflight_decisions SELECT allowed FROM gcra_hot_baseline.charge('check', 'concurrent', 60000000, 8);
