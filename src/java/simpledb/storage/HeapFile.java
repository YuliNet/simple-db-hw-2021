package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pageNo = pid.getPageNumber();

        try (RandomAccessFile file = new RandomAccessFile(f, "r");){
            if ((pageNo + 1) * BufferPool.getPageSize() > f.length())
                throw new IllegalStateException(String.format("illegal page number, please check, pid is %d", pageNo));
            byte[] pageDate = new byte[BufferPool.getPageSize()];
            file.seek(pageNo * BufferPool.getPageSize());
            int readLength = file.read(pageDate, 0, BufferPool.getPageSize());
            if (readLength != BufferPool.getPageSize())
                throw new IllegalStateException(String.format("some mistake in read data"));

            HeapPage page = new HeapPage(new HeapPageId(tableId, pageNo), pageDate);
            return page;
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("read page fail, please check the page number");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNo = page.getId().getPageNumber();
        if (pageNo > numPages()) {
            throw new IllegalArgumentException("illegal page number");
        }
        int pageSize = BufferPool.getPageSize();
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.seek(pageNo * pageSize);
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(f.length() * 1.0 / BufferPool.getPageSize() * 1.0);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // 如果存在某个页面可以放下一个tuple
        List<Page> pageList = new ArrayList<>();
        for (int i = 0; i < numPages(); i ++) {
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (p.getNumEmptySlots() == 0) {
                continue;
            }
            p.insertTuple(t);
            pageList.add(p);
            return pageList;
        }

        // 如果表中所有的页面都满了，则创建新的page到file中
        // 创建成功之后，再次调用getPage方法，会从file的最新位置重新加载一个page进来
        // 注意这里调用 numPages方法的时候，会重新计算file中数据的大小
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bos.write(emptyData);
        bos.close();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), this.numPages() - 1), Permissions.READ_WRITE);
        p.insertTuple(t);
        pageList.add(p);
        return pageList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList = new ArrayList<>();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        pageList.add(p);
        return pageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileInterator(this, tid);
    }

    public static final class HeapFileInterator implements DbFileIterator {

        private HeapFile f;
        private int witchPage;
        private Iterator<Tuple> it;
        private TransactionId tid;

        public HeapFileInterator(HeapFile f, TransactionId tid) {
            this.f = f;
            this.tid = tid;
            this.it = null;
        }

        public Iterator<Tuple> getIt() {
            return this.it;
        }

        @Override
        public void close() {
            it = null;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            int numPages = f.numPages();
            if (witchPage < 0)
                throw new DbException(String.format("illegal page number, witch is : %d", witchPage));
            if (it == null || witchPage >= numPages) 
                return false;
            if (!it.hasNext()) {
                if (witchPage + 1 < numPages) {
                    it = getTupleIterator(witchPage + 1);
                    witchPage += 1;
                    return hasNext();
                }
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext())
                throw new NoSuchElementException("no tuple in this page file");
            return it.next();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            witchPage = 0;
            it = getTupleIterator(witchPage);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        private Iterator<Tuple> getTupleIterator(int pageNo) throws DbException, TransactionAbortedException {
            if (pageNo < 0 || pageNo >= f.numPages())
                throw new DbException(String.format("illegal page number, witch is : %d", pageNo));
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), pageNo), Permissions.READ_WRITE);
            return page.iterator();
        }
    }

}

