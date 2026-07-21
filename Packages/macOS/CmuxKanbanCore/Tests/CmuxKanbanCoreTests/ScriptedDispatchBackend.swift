import Foundation

@testable import CmuxKanbanCore

/// A test ``DispatchBackend`` that replays a fixed script of progress events.
///
/// When `finishes` is true the stream emits the script and completes (modelling
/// a run that ends on its own). When false the stream stays open after the
/// script so a test can exercise ``KanbanEngine/cancel(cardId:)`` against a
/// still-running card; ``cancel(_:)`` then finishes the open stream.
actor ScriptedDispatchBackend: DispatchBackend {
    private let script: [KanbanDispatchProgress]
    private let finishes: Bool
    /// When true, ``dispatch(card:workingDirectory:)`` suspends inside its body
    /// until ``resumeDispatch()`` is called, so a test can interleave a cancel
    /// while the engine is parked on `await backend.dispatch(...)`.
    private let gated: Bool
    private var open: [KanbanDispatchHandle: AsyncStream<KanbanDispatchProgress>.Continuation] = [:]
    private(set) var cancelledHandles: [KanbanDispatchHandle] = []

    /// Continuations resumed by ``resumeDispatch()``; signals that
    /// ``dispatch(card:workingDirectory:)`` has parked.
    private var gateContinuations: [CheckedContinuation<Void, Never>] = []
    /// Set once ``dispatch(card:workingDirectory:)`` has parked on the gate, so a
    /// test can `await` until the engine is actually suspended before cancelling.
    private var parkedContinuations: [CheckedContinuation<Void, Never>] = []
    private var hasParked = false

    init(script: [KanbanDispatchProgress], finishes: Bool = true, gated: Bool = false) {
        self.script = script
        self.finishes = finishes
        self.gated = gated
    }

    func dispatch(card: KanbanCard, workingDirectory: String?) async throws -> KanbanDispatchSession {
        if gated {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                gateContinuations.append(continuation)
                hasParked = true
                let waiters = parkedContinuations
                parkedContinuations.removeAll()
                for waiter in waiters { waiter.resume() }
            }
        }
        let handle = KanbanDispatchHandle(cardId: card.id)
        var continuation: AsyncStream<KanbanDispatchProgress>.Continuation!
        let stream = AsyncStream<KanbanDispatchProgress> { continuation = $0 }
        for event in script { continuation.yield(event) }
        if finishes {
            continuation.finish()
        } else {
            open[handle] = continuation
        }
        return KanbanDispatchSession(handle: handle, progress: stream)
    }

    /// Suspends until ``dispatch(card:workingDirectory:)`` has parked on the gate.
    func waitUntilDispatchParked() async {
        if hasParked { return }
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            parkedContinuations.append(continuation)
        }
    }

    /// Releases a gated ``dispatch(card:workingDirectory:)`` so it returns.
    func resumeDispatch() {
        let pending = gateContinuations
        gateContinuations.removeAll()
        for continuation in pending { continuation.resume() }
    }

    func cancel(_ handle: KanbanDispatchHandle) async {
        cancelledHandles.append(handle)
        open[handle]?.finish()
        open[handle] = nil
    }

    func cancelledCount() -> Int { cancelledHandles.count }
}

/// A test ``DispatchBackend`` whose ``dispatch(card:workingDirectory:)`` always
/// throws, modelling a run that cannot start at all (e.g. the agent executable
/// cannot be resolved). Exercises ``KanbanEngine``'s start-failure path.
struct ThrowingDispatchBackend: DispatchBackend {
    struct DispatchError: Error, CustomStringConvertible {
        var description: String { "could not start" }
    }

    func dispatch(card: KanbanCard, workingDirectory: String?) async throws -> KanbanDispatchSession {
        throw DispatchError()
    }

    func cancel(_ handle: KanbanDispatchHandle) async {}
}
