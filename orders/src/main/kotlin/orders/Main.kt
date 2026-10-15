package orders

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.application.ApplicationCall
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.UUID
import javax.sql.DataSource
import kotlin.random.Random
import kotlin.system.exitProcess
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

private val log = LoggerFactory.getLogger("orders")
private val json = Json { ignoreUnknownKeys = true }

private const val HANDLER_BUDGET_MS = 600L

private const val HANDLER_LEASE_SECONDS = 2

private const val WORKER_LEASE_SECONDS = 10

private const val WORKER_BATCH = 10

private const val SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)"""

private val MIGRATIONS = listOf(
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "CREATE INDEX IF NOT EXISTS orders_pending_idx ON orders (next_attempt_at) WHERE status = 'pending'",
)

@Serializable
data class Item(
    val sku: String,
    val qty: Int,
    @SerialName("price_cents") val priceCents: Long,
)

@Serializable
data class OrderRequest(
    @SerialName("user_id") val userId: String,
    val items: List<Item>,
)

class PendingOrder(
    val id: String,
    val amount: Long,
    val items: List<Item>,
    val attempts: Int,
)

class PaymentException(message: String) : Exception(message)

class App(
    private val ds: DataSource,
    private val client: HttpClient,
    private val paymentUrl: String,
    private val inventoryUrl: String,
) {
    private val paymentBreaker = CircuitBreaker("payment")

    // ---------- HTTP-обработчики ----------

        suspend fun createOrder(call: ApplicationCall) {
        val req = try {
            json.decodeFromString<OrderRequest>(call.receiveText())
        } catch (e: Exception) {
            null
        }
        if (req == null || !valid(req)) {
            call.respondJson(HttpStatusCode.BadRequest, buildJsonObject { put("error", "bad_request") })
            return
        }
        val id = UUID.randomUUID().toString()
        val amount = req.items.sumOf { it.qty * it.priceCents }

        insertOrder(id, req, amount)

        var circuitOpen = false
        val status: String? = withTimeoutOrNull(HANDLER_BUDGET_MS) {
            try {
                tryComplete(id, amount, req.items)
            } catch (e: CircuitOpenException) {
                circuitOpen = true
                null
            }
        }
        if (status != null) {
            updateStatus(id, status)
        } else if (!circuitOpen) {
            markTried(id) 
        }

        val answer = status ?: "pending"
        log.info("order id={} status={}", id, answer)
        call.respondJson(HttpStatusCode.Created, buildJsonObject {
            put("id", id)
            put("status", answer)
        })
    }

    suspend fun getOrder(call: ApplicationCall) {
        val id = call.parameters["id"] ?: ""
        val row = withContext(Dispatchers.IO) {
            ds.connection.use { c ->
                c.prepareStatement("SELECT status, amount_cents FROM orders WHERE id = ?").use { st ->
                    st.setString(1, id)
                    st.executeQuery().use { rs ->
                        if (rs.next()) rs.getString(1) to rs.getLong(2) else null
                    }
                }
            }
        }
        if (row == null) {
            call.respondJson(HttpStatusCode.NotFound, buildJsonObject { put("error", "not_found") })
        } else {
            call.respondJson(HttpStatusCode.OK, buildJsonObject {
                put("id", id)
                put("status", row.first)
                put("amount_cents", row.second)
            })
        }
    }

    // ---------- Бизнес-логика ----------

        private suspend fun tryComplete(id: String, amount: Long, items: List<Item>): String? =
        try {
            reserve(id, items)
            if (charge(id, amount)) {
                release(id)
                "rejected"
            } else {
                "paid"
            }
        } catch (e: CircuitOpenException) {
            throw e
        } catch (e: CancellationException) {
            currentCoroutineContext().ensureActive()
            log.warn("заказ {}: таймаут клиента: {}", id, e.toString())
            null
        } catch (e: Exception) {
            log.warn("заказ {}: итог пока неизвестен: {}", id, e.toString())
            null
        }

    private suspend fun reserve(id: String, items: List<Item>) {
        val body = buildJsonObject {
            put("order_id", id)
            putJsonArray("items") {
                items.forEach { item ->
                    add(buildJsonObject {
                        put("sku", item.sku)
                        put("qty", item.qty)
                    })
                }
            }
        }
        val code = status(HttpMethod.Post, "$inventoryUrl/reservations", body)
        if (code != 200 && code != 201) throw Exception("inventory: $code")
    }

    private suspend fun release(id: String) {
        val code = status(HttpMethod.Delete, "$inventoryUrl/reservations/$id", null)
        if (code != 200 && code != 204) throw Exception("inventory release: $code")
    }

    
    private suspend fun charge(id: String, amount: Long): Boolean {
        val code = paymentBreaker.execute {
            val c = status(
                HttpMethod.Post, "$paymentUrl/payments",
                buildJsonObject {
                    put("order_id", id)
                    put("amount_cents", amount)
                    put("currency", "RUB")
                },
                headers = mapOf("Idempotency-Key" to id),
            )
            if (c >= 500) throw PaymentException("payment: $c")
            c
        }
        return when (code) {
            200, 201 -> false
            402 -> true
            else -> throw PaymentException("payment: $code")
        }
    }

    private suspend fun status(
        method: HttpMethod,
        url: String,
        body: JsonObject?,
        headers: Map<String, String> = emptyMap(),
    ): Int =
        client.request(url) {
            this.method = method
            headers.forEach { (k, v) -> header(k, v) }
            if (body != null) {
                contentType(ContentType.Application.Json)
                setBody(body.toString())
            }
        }.status.value

    // ---------- Воркер ----------

    fun startWorker(scope: CoroutineScope) = scope.launch {
        log.info("воркер запущен")
        while (isActive) {
            if (!paymentBreaker.canPass()) {
                delay(300)
                continue
            }
            val batch = try {
                claimDue(WORKER_BATCH)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("воркер: не удалось взять заказы: {}", e.toString())
                emptyList()
            }
            if (batch.isEmpty()) {
                delay(300)
                continue
            }
            coroutineScope {
                batch.forEach { order -> launch { processPending(order) } }
            }
        }
    }

    private suspend fun processPending(o: PendingOrder) {
        try {
            val status = tryComplete(o.id, o.amount, o.items)
            if (status != null) {
                updateStatus(o.id, status)
                log.info("воркер: order id={} status={} (попытка {})", o.id, status, o.attempts + 1)
            } else {
                scheduleRetry(o.id, o.attempts + 1)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: CircuitOpenException) {
            deferRetry(o.id, 1000)
        } catch (e: Exception) {
            log.warn("воркер: ошибка обработки заказа {}: {}", o.id, e.toString())
        }
    }

    private fun backoffMs(attempts: Int): Long =
        (500L shl attempts.coerceIn(0, 4)).coerceAtMost(5000L) + Random.nextLong(0, 250)

    // ---------- База ----------

    private suspend fun insertOrder(id: String, req: OrderRequest, amount: Long) =
        withContext(Dispatchers.IO) {
            ds.connection.use { c ->
                c.prepareStatement(
                    "INSERT INTO orders (id, user_id, status, amount_cents, items, next_attempt_at) " +
                        "VALUES (?, ?, 'pending', ?, ?::jsonb, now() + interval '$HANDLER_LEASE_SECONDS seconds')"
                ).use { st ->
                    st.setString(1, id)
                    st.setString(2, req.userId)
                    st.setLong(3, amount)
                    st.setString(4, json.encodeToString(req.items))
                    st.executeUpdate()
                }
            }
        }

    private suspend fun updateStatus(id: String, status: String) =
        withContext(Dispatchers.IO) {
            ds.connection.use { c ->
                c.prepareStatement("UPDATE orders SET status = ? WHERE id = ? AND status = 'pending'").use { st ->
                    st.setString(1, status)
                    st.setString(2, id)
                    st.executeUpdate()
                }
            }
        }

    private suspend fun scheduleRetry(id: String, attempts: Int) =
        withContext(Dispatchers.IO) {
            ds.connection.use { c ->
                c.prepareStatement(
                    "UPDATE orders SET attempts = ?, next_attempt_at = now() + make_interval(secs => ?) " +
                        "WHERE id = ? AND status = 'pending'"
                ).use { st ->
                    st.setInt(1, attempts)
                    st.setDouble(2, backoffMs(attempts) / 1000.0)
                    st.setString(3, id)
                    st.executeUpdate()
                }
            }
        }

    private suspend fun markTried(id: String) =
        withContext(Dispatchers.IO) {
            ds.connection.use { c ->
                c.prepareStatement(
                    "UPDATE orders SET attempts = 1 WHERE id = ? AND status = 'pending' AND attempts = 0"
                ).use { st ->
                    st.setString(1, id)
                    st.executeUpdate()
                }
            }
        }

    private suspend fun deferRetry(id: String, delayMs: Long) =
        withContext(Dispatchers.IO) {
            ds.connection.use { c ->
                c.prepareStatement(
                    "UPDATE orders SET next_attempt_at = now() + make_interval(secs => ?) " +
                        "WHERE id = ? AND status = 'pending'"
                ).use { st ->
                    st.setDouble(1, delayMs / 1000.0)
                    st.setString(2, id)
                    st.executeUpdate()
                }
            }
        }

    private suspend fun claimDue(limit: Int): List<PendingOrder> =
        withContext(Dispatchers.IO) {
            ds.connection.use { c ->
                c.prepareStatement(
                    "UPDATE orders SET next_attempt_at = now() + interval '$WORKER_LEASE_SECONDS seconds' " +
                        "WHERE id IN (" +
                        "  SELECT id FROM orders WHERE status = 'pending' AND next_attempt_at <= now() " +
                        "  ORDER BY attempts DESC, created_at LIMIT ? FOR UPDATE SKIP LOCKED) " +
                        "RETURNING id, amount_cents, items::text, attempts"
                ).use { st ->
                    st.setInt(1, limit)
                    st.executeQuery().use { rs ->
                        buildList {
                            while (rs.next()) {
                                add(
                                    PendingOrder(
                                        id = rs.getString(1),
                                        amount = rs.getLong(2),
                                        items = json.decodeFromString<List<Item>>(rs.getString(3)),
                                        attempts = rs.getInt(4),
                                    )
                                )
                            }
                        }
                    }
                }
            }
        }
}

private fun valid(r: OrderRequest): Boolean =
    r.userId.isNotEmpty() && r.items.isNotEmpty() &&
        r.items.all { it.sku.isNotEmpty() && it.qty >= 1 && it.priceCents >= 0 }

private suspend fun ApplicationCall.respondJson(status: HttpStatusCode, body: JsonObject) =
    respondText(body.toString(), ContentType.Application.Json, status)

private fun env(name: String): String =
    System.getenv(name)?.takeIf { it.isNotBlank() } ?: run {
        log.error("не задана переменная окружения {}", name)
        exitProcess(1)
    }

private fun connect(rawUrl: String): HikariDataSource {
    val jdbcUrl: String
    var user = ""
    var pass = ""
    if (rawUrl.startsWith("jdbc:")) {
        jdbcUrl = rawUrl
    } else {
        val uri = URI(rawUrl)
        uri.userInfo?.split(":", limit = 2)?.let {
            user = it[0]
            pass = it.getOrElse(1) { "" }
        }
        val port = if (uri.port > 0) uri.port else 5432
        jdbcUrl = "jdbc:postgresql://${uri.host}:$port${uri.path}" + (uri.query?.let { "?$it" } ?: "")
    }
    var last: Exception? = null
    repeat(60) {
        var ds: HikariDataSource? = null
        try {
            val cfg = HikariConfig().apply {
                this.jdbcUrl = jdbcUrl
                if (user.isNotEmpty()) username = user
                if (pass.isNotEmpty()) password = pass
                maximumPoolSize = 20
            }
            ds = HikariDataSource(cfg)
            ds.connection.use { c ->
                c.createStatement().use { st ->
                    st.execute(SCHEMA)
                    MIGRATIONS.forEach { st.execute(it) }
                }
            }
            return ds
        } catch (e: Exception) {
            ds?.close()
            last = e
            log.info("база недоступна, жду: {}", e.message)
            Thread.sleep(1000)
        }
    }
    throw last!!
}

fun main() {
    val ds = connect(env("DATABASE_URL"))
    val client = HttpClient(CIO) {
        install(HttpTimeout) {
            connectTimeoutMillis = 1000
            requestTimeoutMillis = 3000
        }
    }
    val app = App(
        ds = ds,
        client = client,
        paymentUrl = env("PAYMENT_URL").trimEnd('/'),
        inventoryUrl = env("INVENTORY_URL").trimEnd('/'),
    )
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    app.startWorker(scope)

    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    log.info("orders: слушаю порт {}", port)
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        routing {
            get("/healthz") { call.respondJson(HttpStatusCode.OK, buildJsonObject { put("status", "ok") }) }
            post("/orders") { app.createOrder(call) }
            get("/orders/{id}") { app.getOrder(call) }
        }
    }.start(wait = true)
}