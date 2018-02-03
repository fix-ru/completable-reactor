package ru.fix.completable.reactor.graph.viewer.gl

import javafx.scene.Scene
import ru.fix.completable.reactor.model.GraphModel
import ru.fix.completable.reactor.model.Source
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Created by Kamil Asfandiyarov.
 */
class GraphViewer {
    var scene: Scene
        private set

    private var actionListeners = CopyOnWriteArrayList<ActionListener>()
    private var graphViewPane: GraphViewPane

    private val shortcuts: MutableMap<Shortcut, ShortcutType> = ConcurrentHashMap()


    val graphModel: GraphModel?
        get() = graphViewPane.graphModel


    init {
        val actionListener = object : ActionListener {
            override fun goToSource(source: Source) {
                for (listener in actionListeners) {
                    listener.goToSource(source)
                }
            }

            override fun goToSubgraph(subgraphPayloadClass: String) {
                for (listener in actionListeners) {
                    listener.goToSubgraph(subgraphPayloadClass)
                }
            }

            override fun coordinatesChanged(coordinateItems: List<CoordinateItem>) {
                for (listener in actionListeners) {
                    listener.coordinatesChanged(coordinateItems)
                }
            }
        }

        graphViewPane = GraphViewPane(actionListener, { this.getShortcut(it) })

        graphViewPane.setPrefSize(700.0, 600.0)

        scene = Scene(graphViewPane)

        scene.stylesheets.add(javaClass.getResource("/css/styles.css").toExternalForm())

        //Shortcuts
        graphViewPane.setOnKeyReleased { keyEvent ->
            shortcuts.forEach { shortcut, shortcutType ->
                if (shortcut.getPredicate().test(keyEvent)) {
                    when (shortcutType) {
                        ShortcutType.GOTO_BUILD_GRAPH -> graphViewPane.graphModel?.buildGraphSource?.let {
                            actionListener.goToSource(it)
                        }
                    }
                }

            }
        }
    }

    fun openGraph(graph: GraphModel) {
        graphViewPane.openGraph(graph)
    }


    fun registerListener(actionListener: ActionListener): GraphViewer {
        actionListeners.add(actionListener)
        return this
    }

    fun setShortcut(shortcutType: ShortcutType, shortcut: Shortcut) {
        shortcuts[shortcut] = shortcutType
    }

    fun getShortcut(shortcutType: ShortcutType): Shortcut? {
        for ((shortcut, type) in shortcuts) {
            if (type == shortcutType) {
                return shortcut
            }
        }
        return null
    }

    interface ActionListener {
        /**
         * Viewer asks IDE to navigate to source code
         *
         * @param source source code location
         */
        fun goToSource(source: Source)

        /**
         * Viewer asks IDE to navigate to subgraph view
         * @param subgraphPayloadType payload class name
         */
        fun goToSubgraph(subgraphPayloadType: String)

        /**
         * Graph nodes coordinates changed
         *
         * @param coordinateItems new coordinates
         */
        fun coordinatesChanged(coordinateItems: List<CoordinateItem>)
    }
}