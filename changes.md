## Dask-expr

# v1.0.5

- Ensure that repr doesn't raise if an operand is a pandas object (:pr:`996`) `Florian Jetter`_
- Allow passing of boolean index for column index in loc (:pr:`995`) `Florian Jetter`_
- Update pyproject.toml (:pr:`994`) `Florian Jetter`_
- Fix SettingWithCopyWarning in _merge.py (:pr:`990`) `Miles`_
- Ensure drop matches column names exactly (:pr:`992`) `Florian Jetter`_
- Support ``prefix`` argument in  ``from_delayed`` (:pr:`991`) `Richard (Rick) Zamora`_
- Visual ANALYZE (:pr:`889`) `Hendrik Makait`_

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
