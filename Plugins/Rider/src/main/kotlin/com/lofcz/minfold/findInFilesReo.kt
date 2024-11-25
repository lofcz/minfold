package com.lofcz.minfold

import com.intellij.ide.SearchTopHitProvider
import com.intellij.ide.ui.search.SearchableOptionProcessor
import com.intellij.openapi.project.Project
import com.intellij.openapi.options.Configurable
import java.util.*
import java.util.function.Consumer

class CustomSearchTopHitProvider : SearchTopHitProvider {
    override fun consumeTopHits(pattern: String, consumer: Consumer<Any>, project: Project?) {
        if (pattern.lowercase(Locale.getDefault()).contains("reo")) {
            consumer.accept(object : Configurable {
                override fun getDisplayName(): String = "Toggle Reo Prefix"
                override fun getHelpTopic(): String? = null
                override fun createComponent() = null
                override fun isModified(): Boolean = false
                override fun apply() {}
            })
        }
    }
}