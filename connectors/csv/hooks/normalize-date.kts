// normalize-date.kts — a cell hook: rewrite legacy "DD/MM/YYYY" into ISO "YYYY-MM-DD".
// KotlinScriptHost injects 'bindings' with a mutable "cells" map (the current row's columns).
@Suppress("UNCHECKED_CAST")
val cells = bindings["cells"] as MutableMap<String, Any?>
val raw = cells["signup_date"] as? String
if (raw != null && Regex("""\d{2}/\d{2}/\d{4}""").matches(raw)) {
    val (d, m, y) = raw.split("/")
    cells["signup_date"] = "$y-$m-$d"
}
