# HRIS Normalizer

Vendor-agnostic. Takes any DataFrame of employee data (Workday, BambooHR, ADP, Gusto, Rippling, Hibob, an internal HR export, a CSV — anything) and maps it to a canonical employee schema usable by downstream HR analytics components.

## Canonical schema

```
employee_id, email, first_name, last_name, full_name,
manager_employee_id, department, job_title, location, country,
employment_status, employment_type,
hire_date, termination_date,
tenure_days, is_active
```

Plus the original source columns with `vendor_` prefix (unless `drop_extra_columns: true`).

## Required field map

`column_map: { canonical_field: source_column }`. Provide one entry per canonical field your vendor exposes; missing fields are filled with `None`.

```yaml
column_map:
  employee_id:         emp_id
  email:               work_email
  first_name:          given_name
  last_name:           family_name
  manager_employee_id: supervisor_id
  department:          dept
  job_title:           position
  location:            office
  country:             country_iso
  employment_status:   status
  employment_type:     emp_type
  hire_date:           start_date
  termination_date:    end_date
```

## Vendor-value normalization

Vendor-specific status / type values are auto-normalized via a default map. Override with `status_map` / `type_map`:

```yaml
status_map:
  A:          active
  T:          terminated
  Active:     active
  TERMINATED: terminated
type_map:
  REG-FT:    full_time
  REG-PT:    part_time
  CONTRACT:  contractor
```

The defaults already cover most common conventions (`active / inactive / terminated / on_leave / pending` for status; `full_time / part_time / contractor / intern / temporary` for type). `case_insensitive_map: true` (default) lowercases + strips before lookup.

## Derived fields

- **`full_name`** — derived from `first_name + last_name` if not directly mapped.
- **`tenure_days`** — `today - hire_date` for active employees, `termination_date - hire_date` for ex-employees, `None` if missing dates.
- **`is_active`** — `True` iff normalized status is `active`.

## Sister components

- `merge_dev_hris_ingestion` — pulls from Merge.dev's unified HRIS API (50+ vendors via one client).
- `hr_metrics` — pre-built HR KPIs: headcount by department, tenure distribution, turnover rate, span of control.
- Vendor-specific ingestion components (planned): `workday_hris_ingestion`, `bamboo_hr_ingestion`, etc.
