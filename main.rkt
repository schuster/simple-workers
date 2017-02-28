#lang racket

;; A simple framework for running a queue of arbitrary tasks on a static number of worker threads

(require racket/async-channel)

;; Spawns num-workers worker threads that each continuously run the next thunk from job-queue (an
;; async-channel). Returns an event that is ready for synchronization when all worker threads have
;; terminated (i.e. when all jobs from the queue are complete).
(define (start-workers num-workers job-queue)
  (define worker-threads
    (for/list ([i num-workers])
      (thread
       (lambda ()
         (let loop ()
           (match (async-channel-try-get job-queue)
             [#f #f]
             [job-thunk
              (job-thunk)
              (loop)]))))))
  ;; TODO: return an event that is done when *all* loops are done
  (apply event-and worker-threads))

;; Example usage:
;;
;; (define job-queue (make-async-channel))
;; (async-channel-put job-queue (lambda () (sleep 5) (printf "5\n")))
;; (async-channel-put job-queue (lambda () (sleep 0) (printf "0\n")))
;; (async-channel-put job-queue (lambda () (sleep 2) (printf "2\n")))
;; (async-channel-put job-queue (lambda () (sleep 1) (printf "1\n")))
;; (sync (start-workers 2 job-queue))
;;
;; On a machine with at least 2 CPU cores, we expect to see: 0, 2, 1, 5

;; Returns an event that is ready for synchronization when all the evs are ready, returning the result
;; of the last ev as the synchronization result
(define (event-and ev1 . evs)
  (match evs
    [(list) ev1]
    [_
     ;; Wait on the first event. Once that is ready, wait on the rest
     (handle-evt ev1 (lambda (dummy) (sync (apply event-and evs))))]))

