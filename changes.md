## Dask-expr

# v1.0.4

- Ensure wrapping an array when comparing to Series works if columns are empty (:pr:`984`) `Florian Jetter`_
- Remove keys() (:pr:`983`) `Patrick Hoefler`_
- Fix some reset_index optimization issues (:pr:`982`) `Patrick Hoefler`_
- Fix concat of series objects with column projection (:pr:`981`) `Patrick Hoefler`_
- Raise better error for repartition on divisions with unknown divisions (:pr:`980`) `Patrick Hoefler`_

# v1.0.3
- Support for dask==2023.3.1

# v1.0.2

- Revert enabling pandas cow (:pr:`974`) `Florian Jetter`_
- Fixup predicate pushdown for query 19 (:pr:`973`) `Patrick Hoefler`_
- Fixup set_index with one partition but more divisions by user (:pr:`972`) `Patrick Hoefler`_
- Implement custom reductions (:pr:`970`) `Patrick Hoefler`_
- Fix unique with shuffle and strings (:pr:`971`) `Patrick Hoefler`_
- Fixup filter pushdown through merges with ands and column reuse (:pr:`969`) `Patrick Hoefler`_

# v1.0.0

Initial stable release
