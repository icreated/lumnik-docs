// Reads "12,5 kg" / "3 pce"; a quantity in any other shape is declared unreadable.
@Suppress("UNCHECKED_CAST")
val cells = bindings["cells"] as MutableMap<String, Any?>
val m = Regex("(\\d+(?:,\\d+)?) (kg|pce)").matchEntire((cells["quantity"] as String).trim())
if (m == null) {
    (bindings["unreadable"] as Runnable).run()    // "env. 4 kg": counted, not guessed
} else {
    cells["qty"] = m.groupValues[1].replace(',', '.')
    cells["unit"] = m.groupValues[2]
}
