package orders

import org.slf4j.LoggerFactory

class CircuitOpenException(name: String) : Exception("circuit '$name' is open")


class CircuitBreaker(
    private val name: String,
    private val failureThreshold: Int = 5,
    private val baseOpenMs: Long = 1_000,
    private val maxOpenMs: Long = 4_000,
) {
    private enum class State { CLOSED, OPEN, HALF_OPEN }
    private enum class Permit { NORMAL, PROBE }

    private val log = LoggerFactory.getLogger("breaker.$name")
    private var state = State.CLOSED
    private var failures = 0
    private var openUntil = 0L
    private var openMs = baseOpenMs

    @Synchronized
    fun canPass(): Boolean = when (state) {
        State.CLOSED -> true
        State.OPEN -> System.currentTimeMillis() >= openUntil
        State.HALF_OPEN -> false
    }

    suspend fun <T> execute(block: suspend () -> T): T {
        val permit = tryAcquire() ?: throw CircuitOpenException(name)
        try {
            val result = block()
            onSuccess(permit)
            return result
        } catch (e: Throwable) {
            onFailure(permit)
            throw e
        }
    }

    @Synchronized
    private fun tryAcquire(): Permit? = when (state) {
        State.CLOSED -> Permit.NORMAL
        State.OPEN ->
            if (System.currentTimeMillis() >= openUntil) {
                state = State.HALF_OPEN
                log.info("{}: half-open, пробный запрос", name)
                Permit.PROBE
            } else {
                null
            }
        State.HALF_OPEN -> null
    }

    @Synchronized
    private fun onSuccess(permit: Permit) {
        when (state) {
            State.CLOSED -> failures = 0
            State.HALF_OPEN -> if (permit == Permit.PROBE) {
                state = State.CLOSED
                openMs = baseOpenMs
                failures = 0
                log.info("{}: closed, сервис ожил", name)
            }
            State.OPEN -> Unit
        }
    }

    @Synchronized
    private fun onFailure(permit: Permit) {
        when (state) {
            State.CLOSED -> {
                failures++
                if (failures >= failureThreshold) trip()
            }
            State.HALF_OPEN -> if (permit == Permit.PROBE) {
                openMs = (openMs * 2).coerceAtMost(maxOpenMs)
                trip()
            }
            State.OPEN -> Unit
        }
    }

    private fun trip() {
        state = State.OPEN
        openUntil = System.currentTimeMillis() + openMs
        failures = 0
        log.warn("{}: open на {} мс", name, openMs)
    }
}