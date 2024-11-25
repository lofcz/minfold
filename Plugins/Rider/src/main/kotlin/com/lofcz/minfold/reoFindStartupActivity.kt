package com.lofcz.minfold

import com.intellij.ide.actions.searcheverywhere.SearchEverywhereContributor
import com.intellij.ide.actions.searcheverywhere.SearchEverywhereContributorFactory
import com.intellij.openapi.actionSystem.AnActionEvent
import com.intellij.openapi.fileTypes.FileTypeRegistry
import com.intellij.openapi.progress.ProgressIndicator
import com.intellij.openapi.project.Project
import com.intellij.openapi.roots.ProjectFileIndex
import com.intellij.openapi.vfs.VirtualFile
import com.intellij.openapi.vfs.VirtualFileManager
import com.intellij.psi.PsiFile
import com.intellij.psi.PsiManager
import com.intellij.psi.search.FilenameIndex
import com.intellij.psi.search.GlobalSearchScope
import com.intellij.util.Processor
import com.intellij.util.indexing.FileBasedIndex
import javax.swing.JList
import javax.swing.ListCellRenderer
import com.intellij.util.indexing.ID
import javax.swing.DefaultListCellRenderer
import java.awt.Component
import javax.swing.JLabel

class CustomSearchContributor(private val project: Project) : SearchEverywhereContributor<Any> {


    private val prefix = ".reo"

    override fun getSearchProviderId(): String = "CustomSearchContributor"

    override fun getGroupName(): String = "Custom Search"

    override fun getSortWeight(): Int = 1000

    override fun showInFindResults(): Boolean = true
    override fun processSelectedItem(selected: Any, modifiers: Int, searchText: String): Boolean {
        // Implementujte akci, která se má provést při výběru položky
        // Například:
        // when (selected) {
        //     is PsiElement -> navigateToElement(selected)
        //     is VirtualFile -> openFile(selected)
        //     // další typy...
        // }
        // return true, pokud byla akce úspěšně provedena, jinak false
        return false
    }

    override fun fetchElements(
        pattern: String,
        progressIndicator: ProgressIndicator,
        consumer: Processor<in Any>
    ) {
        val modifiedPattern = prefix + pattern

        val psiManager = PsiManager.getInstance(project)
        val fileIndex = ProjectFileIndex.getInstance(project)

        fileIndex.iterateContent { virtualFile ->
            if (progressIndicator.isCanceled) return@iterateContent false

            if (!virtualFile.isDirectory) {
                val psiFile = psiManager.findFile(virtualFile)
                if (psiFile != null) {
                    processFile(psiFile, modifiedPattern, consumer)
                }
            }
            true
        }
    }

    private fun processFile(
        psiFile: PsiFile,
        pattern: String,
        consumer: Processor<in Any>
    ) {
        val content = psiFile.text
        if (content.contains(pattern, ignoreCase = true)) {
            consumer.process(SearchResult(psiFile, pattern))
        }
    }

    data class SearchResult(val psiFile: PsiFile, val matchedPattern: String)


    override fun getElementsRenderer(): ListCellRenderer<Any> {
        return object : ListCellRenderer<Any> {
            private val defaultRenderer = DefaultListCellRenderer()

            override fun getListCellRendererComponent(
                list: JList<out Any>?,
                value: Any?,
                index: Int,
                isSelected: Boolean,
                cellHasFocus: Boolean
            ): Component {
                val component = defaultRenderer.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus)
                if (component is JLabel && value is SearchResult) {
                    component.text = "${value.psiFile.name} - ${value.matchedPattern}"
                }
                return component
            }
        }
    }

    override fun getDataForItem(element: Any, dataId: String): Any? {
        // Implementujte získávání dat pro položku
        return null
    }

    class Factory : SearchEverywhereContributorFactory<Any> {
        override fun createContributor(initEvent: AnActionEvent): SearchEverywhereContributor<Any> {
            val project = initEvent.project ?: throw IllegalStateException("Project is null")
            return CustomSearchContributor(project)
        }
    }
}
