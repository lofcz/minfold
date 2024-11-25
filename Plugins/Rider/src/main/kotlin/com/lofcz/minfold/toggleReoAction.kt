package com.lofcz.minfold

import com.intellij.find.FindManager
import com.intellij.find.findInProject.FindInProjectManager
import com.intellij.icons.AllIcons
import com.intellij.openapi.actionSystem.AnActionEvent
import com.intellij.openapi.actionSystem.ToggleAction
import com.intellij.openapi.project.DumbAware
import com.intellij.openapi.project.Project

class ToggleReoAction : ToggleAction(
    "Use Reo Search",
    "Use custom Reo search engine",
    AllIcons.Actions.Find
), DumbAware {
    companion object {
        var isEnabled = false
    }

    override fun isSelected(e: AnActionEvent): Boolean {
        return isEnabled
    }

    override fun setSelected(e: AnActionEvent, state: Boolean) {
        isEnabled = state
    }
}