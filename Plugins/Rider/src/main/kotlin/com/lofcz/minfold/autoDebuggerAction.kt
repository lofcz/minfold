package com.lofcz.minfold

import com.intellij.execution.ExecutionListener
import com.intellij.execution.ExecutionManager
import com.intellij.execution.process.ProcessAdapter
import com.intellij.execution.process.ProcessEvent
import com.intellij.execution.process.ProcessHandler
import com.intellij.execution.process.ProcessListener
import com.intellij.execution.runners.ExecutionEnvironment
import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
import com.intellij.openapi.actionSystem.AnActionEvent
import com.intellij.openapi.actionSystem.Presentation
import com.intellij.openapi.actionSystem.ex.CustomComponentAction
import com.intellij.openapi.project.DumbAwareAction
import com.intellij.openapi.project.Project
import com.intellij.openapi.wm.WindowManager
import com.intellij.util.messages.MessageBusConnection
import javax.swing.JComponent

class AutoDebuggerAction : DumbAwareAction("Auto Attach Debugger"), CustomComponentAction {
    private var isActive = false
    private var connection: MessageBusConnection? = null

    override fun actionPerformed(e: AnActionEvent) {
        isActive = !isActive
        updatePresentation(e.presentation)
        updateStatusBar(e.project)
        val project = e.project ?: return

        if (isActive) {
            stopAutoDebugging(project)
        } else {
            startAutoDebugging(project)
        }
    }

    private fun updatePresentation(presentation: Presentation) {
        presentation.text = if (isActive) "Disable Auto Debugger" else "Enable Auto Debugger"
        presentation.description = if (isActive) "Click to disable automatic debugger attachment" else "Click to enable automatic debugger attachment"
    }

    override fun createCustomComponent(presentation: Presentation, place: String): JComponent {
        updatePresentation(presentation)
        return super.createCustomComponent(presentation, place).apply {
            toolTipText = presentation.description
        }
    }

    private fun updateStatusBar(project: Project?) {
        project?.let {
            val statusBar = WindowManager.getInstance().getStatusBar(it)
            statusBar?.info = if (isActive) "Auto Debugger: Active" else "Auto Debugger: Inactive"
        }
    }

    private fun startAutoDebugging(project: Project) {
        connection = project.messageBus.connect()
        connection?.subscribe(ExecutionManager.EXECUTION_TOPIC, object : ExecutionListener {
            override fun processStarted(executorId: String, env: ExecutionEnvironment, handler: ProcessHandler) {
                //val pid = handler.pid ()
                //showNotification(project, "New Process", "Process started with PID: $pid")
                // Zde byste implementovali logiku pro připojení debuggeru
            }
        })
        isActive = true
        showNotification(project, "Auto Debugger", "Auto Debugger started")
    }

    private fun stopAutoDebugging(project: Project) {
        project.messageBus.connect().disconnect()
        isActive = false
        showNotification(project, "Auto Debugger", "Auto Debugger stopped")
    }

    private fun createProcessListener(project: Project): ProcessListener {
        return object : ProcessAdapter() {
            override fun startNotified(event: ProcessEvent) {
                val processHandler = event.processHandler
                //val pid = processHandler.pid() ?: return
                //showNotification(project, "New Process", "Process started with PID: $pid")
                // Zde byste implementovali logiku pro připojení debuggeru
            }
        }
    }

    private fun showNotification(project: Project, title: String, content: String) {
        NotificationGroupManager.getInstance()
            .getNotificationGroup("lofcz.notification.test")
            .createNotification(title, content, NotificationType.INFORMATION)
            .notify(project)
    }
}
