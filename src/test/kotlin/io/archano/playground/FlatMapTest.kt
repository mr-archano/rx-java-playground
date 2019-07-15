package io.archano.playground

import com.google.common.truth.Truth.assertThat
import io.reactivex.Observable
import io.reactivex.disposables.Disposable
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import org.junit.Test

class FlatMapTest {

    @Test
    fun standard() {
        Observable.just(1)
                .doOnSubscribe(printOnSubscribed)
                .doOnNext(printOnNext)
                .flatMap { n ->
                    println("flatMap $n on: $threadName")
                    Observable.just(n)
                            .map { it * 2 }
                            .doOnNext(printOnNext)
                }
                .map { n -> "$n.$n" }
                .doOnNext(printOnNext)
                .subscribeOn(background)
                .observeOn(main)
                .subscribe {
                    println("onNext($it) on: $threadName")
                }

        Thread.sleep(1000L)

        assertThat(output).isEqualTo("""
            subscribed on: RxCachedThreadScheduler-1
            emitted 1 on: RxCachedThreadScheduler-1
            flatMap 1 on: RxCachedThreadScheduler-1
            emitted 2 on: RxCachedThreadScheduler-1
            emitted 2.2 on: RxCachedThreadScheduler-1
            onNext(2.2) on: RxSingleScheduler-1
            """.trimIndent())
    }

    @Test
    fun wrong() {
        Observable.just(1)
                .doOnSubscribe(printOnSubscribed)
                .doOnNext(printOnNext)
                .observeOn(main)
                .flatMap { n ->
                    println("flatMap $n on: $threadName")
                    Observable.just(n)
                            .map { it * 2 }
                            .doOnNext(printOnNext)
                }
                .map { n -> "$n.$n" }
                .doOnNext(printOnNext)
                .subscribeOn(background)
                .observeOn(main)
                .subscribe {
                    println("onNext($it) on: $threadName")
                }

        Thread.sleep(1000L)

        assertThat(output).isEqualTo("""
            subscribed on: RxCachedThreadScheduler-1
            emitted 1 on: RxCachedThreadScheduler-1
            flatMap 1 on: RxSingleScheduler-1
            emitted 2 on: RxSingleScheduler-1
            emitted 2.2 on: RxSingleScheduler-1
            onNext(2.2) on: RxSingleScheduler-1
            """.trimIndent())
    }

    @Test
    fun variation1() {
        Observable.just(1)
                .doOnSubscribe(printOnSubscribed)
                .doOnNext(printOnNext)
                .flatMap { n ->
                    println("flatMap $n on: $threadName")
                    Observable.just(n)
                            .map { it * 2 }
                            .doOnNext(printOnNext)
                            .subscribeOn(background)
                }
                .map { n -> "$n.$n" }
                .doOnNext(printOnNext)
                .subscribeOn(background)
                .observeOn(main)
                .subscribe {
                    println("onNext($it) on: $threadName")
                }

        Thread.sleep(1000L)

        assertThat(output).isEqualTo("""
            subscribed on: RxCachedThreadScheduler-1
            emitted 1 on: RxCachedThreadScheduler-1
            flatMap 1 on: RxCachedThreadScheduler-1
            emitted 2 on: RxCachedThreadScheduler-2
            emitted 2.2 on: RxCachedThreadScheduler-2
            onNext(2.2) on: RxSingleScheduler-1
            """.trimIndent())
    }

    @Test
    fun variation2() {
        Observable.just(1)
                .doOnSubscribe(printOnSubscribed)
                .doOnNext(printOnNext)
                .observeOn(main)
                .flatMap { n ->
                    println("flatMap $n on: $threadName")
                    Observable.just(n)
                            .map { it * 2 }
                            .doOnNext(printOnNext)
                            .subscribeOn(background)
                }
                .map { n -> "$n.$n" }
                .doOnNext(printOnNext)
                .subscribeOn(background)
                .observeOn(main)
                .subscribe {
                    println("onNext($it) on: $threadName")
                }

        Thread.sleep(1000L)

        assertThat(output).isEqualTo("""
            subscribed on: RxCachedThreadScheduler-1
            emitted 1 on: RxCachedThreadScheduler-1
            flatMap 1 on: RxSingleScheduler-1
            emitted 2 on: RxCachedThreadScheduler-2
            emitted 2.2 on: RxCachedThreadScheduler-2
            onNext(2.2) on: RxSingleScheduler-1
            """.trimIndent())
    }

    @Test
    fun `variation subject 1`() {
        val consumer = PublishSubject.create<Int>()
        val data = Observable.just(1)
                .doOnNext(printOnNext)
                .flatMap { n ->
                    println("flatMap $n on: $threadName")
                    Observable.just(n)
                            .map { it * 2 }
                            .doOnNext(printOnNext)
                }
                .publish()

        data.subscribe(consumer)

        consumer
                .doOnSubscribe(printOnSubscribed)
                .subscribeOn(background)
                .observeOn(main)
                .subscribe {
                    println("onNext($it) on: $threadName")
                }

        data.connect()

        Thread.sleep(1000L)

        assertThat(output).isEqualTo("""
            subscribed on: RxCachedThreadScheduler-1
            emitted 1 on: Test worker
            flatMap 1 on: Test worker
            emitted 2 on: Test worker
            onNext(2) on: RxSingleScheduler-1
            """.trimIndent())
    }

    private val main get() = Schedulers.single()
    private val background get() = Schedulers.io()

    private val output: String get() = buffer.trim()
    private var buffer = ""
    private fun println(txt: String) {
        buffer += txt + "\n"
    }

    private val threadName get() = Thread.currentThread().name
    private val printOnSubscribed: (Disposable) -> Unit = { println("subscribed on: $threadName") }
    private val printOnNext: (Any) -> Unit = { println("emitted $it on: $threadName") }

}
