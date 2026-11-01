INSERT INTO gcra_hot_fill80.preflight_decisions SELECT allowed FROM gcra_hot_fill80.charge('check', 'concurrent', 60000000, 8);
