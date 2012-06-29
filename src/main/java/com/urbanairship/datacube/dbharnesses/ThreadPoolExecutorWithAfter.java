//package com.urbanairship.datacube.dbharnesses;
//
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ThreadFactory;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
//
//public class ThreadPoolExecutorWithAfter<T extends Op> extends ThreadPoolExecutor {
////    private final AfterExecute runAfterExecution;
////    
////    public ThreadPoolExecutorWithAfter(int corePoolSize, int maxPoolSize, long keepAliveTime, 
////            TimeUnit keepAliveUnit, BlockingQueue<Runnable> queue, ThreadFactory threadFactory, 
////            AfterExecute runAfterExecutionCallback) {
////        super(corePoolSize, maxPoolSize, keepAliveTime, keepAliveUnit, queue, threadFactory);
////        this.runAfterExecution = runAfterExecutionCallback;
////    }
////
////    @Override
////    protected void afterExecute(Runnable r, Throwable t) {
////        super.afterExecute(r, t);
////        runAfterExecution.afterExecute(r, t);
////    }
//     
//    
//}
