# Canonical Naming

All generated feature columns use the Qualud canonical naming grammar.

`timestamp` remains the date column and is not renamed.

## Demand

Global total:

```text
DEMAND__TOTAL
```

Global categorical feature:

```text
DEMAND__{VARIABLE}__{CATEGORY}
```

RS feature:

```text
DEMAND__{VARIABLE}__{CATEGORY}__RS__{GEO}
DEMAND__TOTAL__RS__{GEO}
```

UP feature:

```text
DEMAND__{VARIABLE}__{CATEGORY}__UP__{GEO}
DEMAND__TOTAL__UP__{GEO}
```

Examples:

```text
DEMAND__SERVEI_CODI__INF
DEMAND__SERVEI_CODI__INF__RS__RS_64
DEMAND__SERVEI_CODI__INF__UP__MICRO_01
DEMAND__TOTAL__RS__RS_64
```

## Diagnosis

Global total:

```text
DIAGNOSIS__TOTAL
```

Diagnosis code or group:

```text
DIAGNOSIS__ICD10_3__{CODE_OR_GROUP}
```

RS diagnosis feature:

```text
DIAGNOSIS__ICD10_3__{CODE_OR_GROUP}__RS__{GEO}
DIAGNOSIS__TOTAL__RS__{GEO}
```

UP diagnosis feature:

```text
DIAGNOSIS__ICD10_3__{CODE_OR_GROUP}__UP__{GEO}
DIAGNOSIS__TOTAL__UP__{GEO}
```

Examples:

```text
DIAGNOSIS__ICD10_3__G01
DIAGNOSIS__ICD10_3__D08
DIAGNOSIS__ICD10_3__G01__RS__RS_67
DIAGNOSIS__ICD10_3__G01__UP__MICRO_01
DIAGNOSIS__TOTAL__UP__MICRO_01
```

## Compatibility

The code includes best-effort canonicalization for legacy columns when joining final files. New outputs should already be canonical.
