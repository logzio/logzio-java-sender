package io.logz.sender.model

data class RTQuery(val id: Int,
                   val title: String,
                   val query: String,
                   val hostname: List<String>?,
                   val tag: List<String>?,
                   val startDate: Long,
                   val endDate: Long) {
    constructor(): this(0, "", "", null, null, 0, 0)
}

