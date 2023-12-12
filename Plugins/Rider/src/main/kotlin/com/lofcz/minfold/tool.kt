package com.lofcz.minfold

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.pgreze.process.Redirect
import com.github.pgreze.process.process
import com.intellij.ide.DataManager
import com.intellij.notification.Notification
import com.intellij.notification.NotificationType
import com.intellij.notification.Notifications
import com.intellij.openapi.components.BaseState
import com.intellij.openapi.components.Service
import com.intellij.openapi.components.SimplePersistentStateComponent
import com.intellij.openapi.components.State
import com.intellij.openapi.project.DumbAware
import com.intellij.openapi.project.Project
import com.intellij.openapi.wm.ToolWindow
import com.intellij.openapi.wm.ToolWindowFactory
import com.intellij.ui.components.JBPasswordField
import com.intellij.ui.components.JBTextField
import com.intellij.ui.components.TextComponentEmptyText
import com.intellij.ui.content.ContentFactory
import com.jetbrains.rider.projectView.solutionDirectory
import com.jetbrains.rider.projectView.solutionName
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import net.miginfocom.swing.MigLayout
import org.apache.commons.lang3.StringUtils
import java.awt.BorderLayout
import java.awt.GridLayout
import java.awt.event.ActionEvent
import java.awt.event.FocusEvent
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import javax.swing.*
import kotlin.math.min


@Service
@State(name="test")
class ToolState : SimplePersistentStateComponent<TestState>(TestState()) {

}

class TestState : BaseState() {
    var text : String = ""
}

class MinfoldSaveDataEntry(connString: String, database: String, location: String, optional: String, projectPath: String, projectName: String) {
    var connString = ""
    var database = ""
    var location = ""
    var optional = ""
    var projectPath = ""
    var projectName = ""

    init {
        this.connString = connString
        this.database = database
        this.location = location
        this.optional = optional
        this.projectName = projectName
        this.projectPath = projectPath
    }

}

class MinfoldSaveData {
    var saves : MutableList<MinfoldSaveDataEntry> = mutableListOf()
}

class MinfoldToolWindowFactory : ToolWindowFactory, DumbAware {

    override fun createToolWindowContent(project: Project, toolWindow: ToolWindow) {
        val toolWindowContent = MinfoldToolWindowFactory(toolWindow, project)

        print("ahoj ahoj");

        val content = ContentFactory.getInstance().createContent(toolWindowContent.rootFrame, "", false)
        toolWindow.contentManager.addContent(content)
    }

    private class SmartPwInput : JBPasswordField() {

        var showPw = false

        override fun processFocusEvent(e: FocusEvent?) {

            showPw = !showPw

            if (!showPw) {
                this.echoChar = '*'
            }
            else {
                this.echoChar = 0.toChar()
            }

            return super.processFocusEvent(e)
        }
    }

    private class MinfoldToolWindowFactory(toolWindow: ToolWindow, project: Project) {

        val rootFrame = JPanel()
        val contentPanel = JPanel()
        private val currentDate = JLabel()
        private val timeZone = JLabel()
        private val currentTime = JLabel()
        private val stdOutLabel = JLabel()
        private val decorateRegex = Regex("^\\<\\|([\\w_-]+),([\\w_-]+)\\|\\>")
        private var errLines = Collections.synchronizedList(mutableListOf<String>())
        private var outputLines = Collections.synchronizedList(mutableListOf<String>())
        private var minfoldFailed = false
        private var mindoldRunning = false

        val project: Project;
        var inputMap: MutableMap<String, com.intellij.ui.TextAccessor> = mutableMapOf()
        var pwMap: MutableMap<String, SmartPwInput> = mutableMapOf()

        private fun SetPlaceholder(field: JBTextField, placeholder: String, key: String) {
            field.putClientProperty(
                    TextComponentEmptyText.STATUS_VISIBLE_FUNCTION,
                    com.intellij.util.BooleanFunction<JBTextField> { x -> x.text.isEmpty() })

            field.emptyText.text = placeholder
            inputMap[key] = field
        }

        private fun SetPlaceholder(field: SmartPwInput, placeholder: String, key: String) {
            field.putClientProperty(
                    TextComponentEmptyText.STATUS_VISIBLE_FUNCTION,
                    com.intellij.util.BooleanFunction<JBTextField> { x -> x.text.isEmpty() })

            field.emptyText.text = placeholder
            pwMap[key] = field
        }

        fun getVersion() : String {
            var version = ""

            runBlocking {
                val res = process(
                        "minfold", "--version",
                        stdout = Redirect.Consume { flow ->
                            run {
                                version = flow.toList().joinToString()
                            }
                        }
                )
            }

            return version
        }

        init {
            this.project = project;

            rootFrame.layout = BorderLayout()

            // versions: https://api.nuget.org/v3-flatcontainer/minfold.cli/index.json
            //rootFrame.add(contentPanel)

            val cPanel = JPanel(MigLayout("fillx", "[fill, grow]"))

            val panelA = JPanel(GridLayout(0, 1))
            panelA.add(JLabel("Database"))


            val migPanel = JPanel(MigLayout("fillx", "[][fill, grow]"))

            fun AddRow(label: String, placeholder: String, key: String, passwordField: Boolean, defaultValue: String?) {
                migPanel.add(JLabel("${label}:"), "trailing,left")

                if (passwordField) {
                    migPanel.add(SmartPwInput().apply { SetPlaceholder(this, placeholder, key); text = defaultValue }, "span,wrap,leading")
                }
                else {
                    migPanel.add(JBTextField().apply { SetPlaceholder(this, placeholder, key); text = defaultValue }, "span,wrap,leading")
                }
            }

            lateinit var minfoldData: MinfoldSaveData
            val mapper = jacksonObjectMapper()

            try {
                val saveData: MinfoldSaveData = mapper.readValue(Files.readString(Paths.get("C:\\ProgramData\\Minfold\\data.json")))
                minfoldData = saveData
            }
            catch (e: Exception) {
                minfoldData = MinfoldSaveData()
            }

            var data : MinfoldSaveDataEntry? = null

            val projectName = project.solutionName
            val projectDir = project.solutionDirectory.absolutePath

            for (x in minfoldData.saves.reversed()) {
                if (projectName == x.projectName && projectDir == x.projectPath) {
                    data = x
                    break
                }
            }

            AddRow("Database", "MyDatabase", "database", false, data?.database)
            AddRow("Connection String", "Data Source=SERVER:PORT;Initial Catalog=DATABASE;User ID=USER;Password=PASSWORD;TrustServerCertificate=True;Encrypt=False", "conn", true, data?.connString)
            AddRow("Code Path", "Path to the folder with a .csproj project", "path", false, data?.location)
            AddRow("Additional parameters", "Optional: --param1 param1Value", "pars", false, data?.optional)

            migPanel.add(createControlsPanel(toolWindow), "left,wrap")

            cPanel.add(migPanel, "span,left,wrap")
            cPanel.add(stdOutLabel, "span,left,wrap")

            rootFrame.add(cPanel, BorderLayout.NORTH)

            var version = ""

            try {
                version = getVersion()
            }
            catch (e: Exception) {
                stdOutLabel.text = "Minfold not detected, installing.."
                stdOutLabel.repaint()

                try {
                    runBlocking {
                        val res = process(
                                "dotnet", "tool", "install", "Minfold.Cli",
                                stdout = Redirect.Consume { flow ->
                                    run {
                                        version = flow.toList().joinToString()
                                    }
                                }
                        )
                    }
                }
                catch (e: Exception) {
                    stdOutLabel.text = "dotnet not installed, please install .NET SDK"
                    stdOutLabel.repaint()
                }

                try {
                    version = getVersion()
                    stdOutLabel.text = "Minfold installed, version: $version"
                    stdOutLabel.repaint()
                }
                catch (e: Exception) {
                    stdOutLabel.text = "Failed to install Minfold: ${e.message}"
                    stdOutLabel.repaint()
                }
            }
        }

        fun translateCommandline(toProcess: String?): Array<String?> {
            if (toProcess == null || toProcess.isEmpty()) {
                return arrayOfNulls(0)
            }

            // parse with a simple finite state machine
            val normal = 0
            val inQuote = 1
            val inDoubleQuote = 2
            var state = normal
            val tok = StringTokenizer(toProcess, "\"\' ", true)
            val result = ArrayList<String?>()
            val current = java.lang.StringBuilder()
            var lastTokenHasBeenQuoted = false

            while (tok.hasMoreTokens()) {
                val nextTok = tok.nextToken()
                when (state) {
                    inQuote -> if ("\'" == nextTok) {
                        lastTokenHasBeenQuoted = true
                        state = normal
                    } else {
                        current.append(nextTok)
                    }

                    inDoubleQuote -> if ("\"" == nextTok) {
                        lastTokenHasBeenQuoted = true
                        state = normal
                    } else {
                        current.append(nextTok)
                    }

                    else -> {
                        if ("\'" == nextTok) {
                            state = inQuote
                        } else if ("\"" == nextTok) {
                            state = inDoubleQuote
                        } else if (" " == nextTok) {
                            if (lastTokenHasBeenQuoted || current.length != 0) {
                                result.add(current.toString())
                                current.setLength(0)
                            }
                        } else {
                            current.append(nextTok)
                        }
                        lastTokenHasBeenQuoted = false
                    }
                }
            }
            if (lastTokenHasBeenQuoted || current.length != 0) {
                result.add(current.toString())
            }
            if (state == inQuote || state == inDoubleQuote) {
                throw RuntimeException("unbalanced quotes in $toProcess")
            }
            return result.toTypedArray<String?>()
        }

        private fun setIconLabel(label: JLabel, imagePath: String) {
            label.setIcon(ImageIcon(Objects.requireNonNull(javaClass.getResource(imagePath))))
        }

        private fun onStdOut() {

            if (outputLines.isEmpty()) {
                stdOutLabel.text = ""
                stdOutLabel.repaint()
                return
            }

            val buff = StringBuilder()
            buff.append("<html><p style='margin-left: 10px; margin-right: 10px;'>")

            for (x in outputLines) {

                val match = decorateRegex.find(x)

                if (match != null && match.groups.count() >= 3) {
                    val stream = match.groups[1]!!.value
                    val group = match.groups[2]!!.value

                    var color = ""

                    if (stream == "err") {
                        color = "color: #FF4136;"
                        minfoldFailed = true
                    }
                    else if (group == "err") {
                        color = "color: #FF4136;"
                        minfoldFailed = true
                    }
                    else if (group == "warn") {
                        color = "color: #FF851B;"
                    }

                    val xMsg = x.substring(match.range.last + 1)
                    buff.append("<span style='$color'>$xMsg</span>")
                    buff.append("<br/>")
                    continue
                }

                buff.append(x)
                buff.append("<br/>")
            }

            buff.append("</p></html>")
            stdOutLabel.text = buff.toString()
            stdOutLabel.repaint()
        }

        private fun onMinfoldClick() {

            if (mindoldRunning) {
                val notification = Notification(
                        "minfold.busy",
                        "Minfold busy",
                        "Running previous command, please wait",
                        NotificationType.INFORMATION
                )
                Notifications.Bus.notify(notification, project)
                return
            }

            mindoldRunning = true
            outputLines.clear()
            errLines.clear()

            onStdOut()

            val db = inputMap["database"]!!.text
            val conn = pwMap["conn"]!!.getPassword().joinToString(separator = "")
            val path = inputMap["path"]!!.text
            val optional = inputMap["pars"]!!.text

            var empty = true
            var decorate = false

            val argList = mutableListOf<String>()

            if (db.isNotEmpty()) {
                argList.add("--database")
                argList.add(db)
                empty = false
            }

            if (conn.isNotEmpty()) {
                argList.add("--connection")
                argList.add(conn.toString())
                empty = false
            }

            if (path.isNotEmpty()) {
                argList.add("--codePath")
                argList.add(path)
                empty = false
            }

            if (optional.isNotEmpty()) {

                for (par in translateCommandline(optional)) {
                    argList.add(optional)
                }

                empty = false
            }

            if (!optional.contains("--stdDecorate") && !optional.contains("--help") && !optional.contains("--version")) {
                decorate = true;
            }

            if (empty) {
                argList.add("--help")
            }

            if (decorate) {
                argList.add("--stdDecorate")
            }

            minfoldFailed = false

            try {
                runBlocking {
                    val res = process(
                            "minfold", *argList.toTypedArray(),
                            stdout = Redirect.Consume { flow -> flow.toList(outputLines); onStdOut() },
                            stderr = Redirect.Consume { flow -> flow.toList(outputLines); onStdOut() }
                    )
                }

                val os = System.getProperty("os.name").lowercase()

                if (os.contains("win")) {
                    if (db.isNotEmpty() && conn.isNotEmpty() && path.isNotEmpty()) {
                        try {

                            val folderPath = Paths.get("C:\\ProgramData\\Minfold")

                            if (!Files.exists(folderPath)) {
                                Files.createDirectory(folderPath)
                            }

                            lateinit var minfoldData: MinfoldSaveData
                            val mapper = jacksonObjectMapper()

                            try {
                                val saveData: MinfoldSaveData = mapper.readValue(Files.readString(Paths.get("C:\\ProgramData\\Minfold\\data.json")))
                                minfoldData = saveData
                            }
                            catch (e: Exception) {
                                minfoldData = MinfoldSaveData()
                            }

                            minfoldData.saves.add(MinfoldSaveDataEntry(conn, db, path, optional, project.solutionDirectory.absolutePath, project.solutionName))
                            minfoldData.saves = minfoldData.saves.takeLast(1000).reversed().toMutableList()

                            val newText = mapper.writeValueAsString(minfoldData)

                            val fw = FileWriter("C:\\ProgramData\\Minfold\\data.json", false)
                            fw.write(newText)
                            fw.close()
                        }
                        catch (e: Exception) {
                            val s = e.message
                        }

                    }
                }
            }
            catch (e: Exception) {
                stdOutLabel.text = "Failed to execute Minfold: ${e.message}"
                stdOutLabel.repaint()
            }

            mindoldRunning = false
        }

        private fun createControlsPanel(toolWindow: ToolWindow): JPanel {
            val controlsPanel = JPanel()
            val left = 0 //-8
            controlsPanel.setBorder(BorderFactory.createEmptyBorder(0, left, 0, 0))

            val minfoldBtn = JButton("Minfold")
            minfoldBtn.addActionListener { e: ActionEvent? -> run {
                onMinfoldClick()
            } }

            controlsPanel.add(minfoldBtn)
            return controlsPanel
        }

        private fun updateCurrentDateTime() {
            val calendar = Calendar.getInstance()
            currentDate.setText(getCurrentDate(calendar))
            timeZone.setText(getTimeZone(calendar))
            currentTime.setText(getCurrentTime(calendar))
        }

        private fun getCurrentDate(calendar: Calendar): String {
            return (calendar[Calendar.DAY_OF_MONTH].toString() + "/"
                    + (calendar[Calendar.MONTH] + 1) + "/"
                    + calendar[Calendar.YEAR])
        }

        private fun getTimeZone(calendar: Calendar): String {
            val gmtOffset = calendar[Calendar.ZONE_OFFSET].toLong() // offset from GMT in milliseconds
            val gmtOffsetString = (gmtOffset / 3600000).toString()
            return if (gmtOffset > 0) "GMT + $gmtOffsetString" else "GMT - $gmtOffsetString"
        }

        private fun getCurrentTime(calendar: Calendar): String {
            return getFormattedValue(calendar, Calendar.HOUR_OF_DAY) + ":" + getFormattedValue(
                    calendar,
                    Calendar.MINUTE
            ) + ":" + getFormattedValue(calendar, Calendar.SECOND)
        }

        private fun getFormattedValue(calendar: Calendar, calendarField: Int): String {
            val value = calendar[calendarField]
            return StringUtils.leftPad(value.toString(), 2, "0")
        }
    }
}
