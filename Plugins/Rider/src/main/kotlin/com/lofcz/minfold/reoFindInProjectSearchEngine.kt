package com.lofcz.minfold

import com.intellij.find.FindInProjectSearchEngine
import com.intellij.find.FindModel
import com.intellij.openapi.project.Project

class ReoFindInProjectSearchEngine : FindInProjectSearchEngine {
    override fun createSearcher(findModel: FindModel, project: Project): FindInProjectSearchEngine.FindInProjectSearcher? {
        if (!ToggleReoAction.isEnabled) {
            return null // Použije se standardní vyhledávač bez modifikace
        }

        // Modifikujeme hledaný řetězec přímo v původním FindModel
        findModel.stringToFind = "reo.${findModel.stringToFind}"

        // Vracíme null, aby se použil standardní vyhledávač, ale s modifikovaným řetězcem
        return null
    }
}