package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private int numPages;

    private Map<PageId, Pair<Page, Permissions>> LRUCache;

    public PageLockManager pageLockManager;


    public class Lock {
        public int lockType;
        public TransactionId tid;
        public Lock(TransactionId tid, int lockType) {
            this.lockType = lockType;
            this.tid = tid;
        }
    }

    public class PageLockManager {
        public Map<PageId, List<Lock>> lockMap;
        public PageLockManager() {
            this.lockMap = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquireLock(PageId pid, TransactionId tid, int lockType) {
            if (!lockMap.containsKey(pid)) {
                List<Lock> list = new ArrayList<>();
                list.add(new Lock(tid, lockType));
                lockMap.put(pid, list);
                return true;
            }
            List<Lock> pageLockList = lockMap.get(pid);
            for (Lock lock : pageLockList) {
                if (lock.tid.equals(tid)) {
                    // 实现可重入锁
                    if (lock.lockType == lockType) {
                        return true;
                    }

                    if (lock.lockType == 1) {
                        return true;
                    }

                    // 实现锁升级
                    if (pageLockList.size() == 1) {
                        lock.lockType = 1;
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            if (pageLockList.get(0).lockType == 1) {
                assert pageLockList.size() == 1 : "exclusive lock can't coexist with other locks";
                return false;
            }

            if (lockType == 0) {
                Lock sharedLock = new Lock(tid, 0);
                pageLockList.add(sharedLock);
                lockMap.put(pid, pageLockList);
                return true;
            }
            return false;
        }

        public synchronized boolean releaseLock(PageId pid, TransactionId tid) {
            assert lockMap.get(pid) != null : "no page in lock";
            List<Lock> pageLockList = lockMap.get(pid);
            Iterator<Lock> lockIterator = pageLockList.iterator();
            while (lockIterator.hasNext()) {
                Lock lock = lockIterator.next();
                if (tid.equals(lock.tid)) {
                    lockIterator.remove();
                    if (pageLockList.size() == 0) {
                        lockMap.remove(pid);
                    }
                    return true;
                }
            }
            return false;
        }

        public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
            if (!lockMap.containsKey(pid)) {
                return false;
            }

            List<Lock> pageLockList = lockMap.get(pid);
            for (Lock lock : pageLockList) {
                if (tid.equals(lock.tid)) {
                    return true;
                }
            }

            return false;
        }
    }
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.LRUCache = new LinkedHashMap<PageId, Pair<Page, Permissions>>(numPages, 0.75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<PageId, Pair<Page, Permissions>> eldest) {
                return size() > numPages;
            }
        };
        this.pageLockManager = new PageLockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     * @throws DeadlockException
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = 0;
        } else {
            lockType = 1;
        }
        boolean lockAcquired = false;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        while (!lockAcquired) {
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new TransactionAbortedException();
            }
            lockAcquired = pageLockManager.acquireLock(pid, tid, lockType);
        }
        Pair<Page, Permissions> newPair = null;
        if (LRUCache.containsKey(pid)) {
            // Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            // Pair<Page, Permissions> pair = Pair.of(page, perm);
            // LRUCache.put(pid, pair);
            Pair<Page, Permissions> pair = LRUCache.get(pid);
            return pair.getLeft();
        }
        // 实现LRU替换策略，从当前Database下，所有表的目录中，获取表的dbfile，根据pid获取dbfile中的page到LRU缓存中来
        newPair = Pair.of(Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid), perm);
        if (LRUCache.size() >= numPages) {
            evictPage();
        }
        LRUCache.put(pid, newPair);
        return newPair.getLeft();
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        pageLockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return pageLockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            if (commit) {
                // flushPages(tid);
                Iterator<Map.Entry<PageId, Pair<Page, Permissions>>> iterator = LRUCache.entrySet().iterator();
                List<PageId> tmpList = new ArrayList<>();
                while (iterator.hasNext()) {
                    Map.Entry<PageId, Pair<Page, Permissions>> entry = iterator.next();
                    PageId pid = entry.getKey();
                    Page page = entry.getValue().getLeft();
                    page.setBeforeImage();
                    if (tid.equals(page.isDirty())) {
                        tmpList.add(pid);
                    }
                }
                for (PageId pid : tmpList) {
                    flushPage(pid);
                }
            } else {
                restorePages(tid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            for (PageId pid : LRUCache.keySet()) {
                if (pageLockManager.holdsLock(pid, tid)) {
                    unsafeReleasePage(tid, pid);
                }
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(file.insertTuple(tid, t), tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(file.deleteTuple(tid, t), tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        List<PageId> tmpList = new ArrayList<>();
        for (PageId pid : LRUCache.keySet()) {
            tmpList.add(pid);
        }
        for (PageId pid : tmpList) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        LRUCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * 开启 write log ahead 机制, flush之前要写日志
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = LRUCache.get(pid).getLeft();
        TransactionId tid = null;
        if ((tid = p.isDirty()) != null) {
            Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<Map.Entry<PageId, Pair<Page, Permissions>>> iterator = LRUCache.entrySet().iterator();
        List<PageId> tmpList = new ArrayList<>();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Pair<Page, Permissions>> entry = iterator.next();
            PageId pid = entry.getKey();
            Page page = entry.getValue().getLeft();
            page.setBeforeImage();
            if (tid.equals(page.isDirty())) {
                tmpList.add(pid);
            }
        }

        for (PageId pid : tmpList) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<Map.Entry<PageId, Pair<Page, Permissions>>> iterator = LRUCache.entrySet().iterator();
        Map.Entry<PageId, Pair<Page, Permissions>> entry = null;
        while (iterator.hasNext()) {
            Map.Entry<PageId, Pair<Page, Permissions>> tmp = iterator.next();
            TransactionId tid = tmp.getValue().getLeft().isDirty();
            if (tid == null) {
                entry = tmp;
                break;
            }
        }
        if (entry == null) {
            throw new DbException("failed to evict page: all pages are either dirty");
        }
        try {
            flushPage(entry.getKey());
        } catch (Exception e) {
            e.printStackTrace();
        }
        discardPage(entry.getKey());
    }

    private void updateBufferPool(List<Page> pageList, TransactionId tid) throws DbException{
        for (Page p : pageList) {
            p.markDirty(true, tid);
            if (!LRUCache.containsKey(p.getId())) {
                if (LRUCache.size() >= numPages) {
                    evictPage();
                }
            }
            LRUCache.put(p.getId(), Pair.of(p, Permissions.READ_WRITE));
        }
    }

    /**
     * 用于abort某个事物操作过的page，实现2pl
     * @see BufferPool#transactionComplete(TransactionId, boolean)
     * @param tid 事务id
     */
    private synchronized void restorePages(TransactionId tid) {
        Iterator<Map.Entry<PageId, Pair<Page, Permissions>>> iterator = LRUCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Pair<Page, Permissions>> entry = iterator.next();
            Page oldPage = entry.getValue().getLeft();
            PageId pid = entry.getKey();
            if (oldPage.isDirty() != null && oldPage.isDirty().equals(tid)) {
                int tableId = pid.getTableId();
                DbFile file = Database.getCatalog().getDatabaseFile(tableId);
                Page fromDiskPage = file.readPage(pid);
                LRUCache.put(pid, Pair.of(fromDiskPage, entry.getValue().getRight()));
            }
        }
    }

}
