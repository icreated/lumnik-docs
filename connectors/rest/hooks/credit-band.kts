// A partner's credit band, from its credit limit: A from 20 000, B from 5 000, C below.
@Suppress("UNCHECKED_CAST")
val cells = bindings["cells"] as MutableMap<String, Any?>
val limit = (cells["credit_limit"] as Number).toLong()
cells["credit_band"] = when {
    limit >= 20_000 -> "A"
    limit >= 5_000 -> "B"
    else -> "C"
}
