package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Drop;
import edu.whu.tmdb.query.operations.utils.MemConnect;

public class DropImpl implements Drop {

    private MemConnect memConnect;

    public DropImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    //这个方法实现了Drop接口中的drop方法，接收一个Statement对象，强制转换为Drop类型，然后调用execute方法执行删除操作。
    @Override
    public boolean drop(Statement statement) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    //从Drop语句中提取表名，获取对应的类ID，并调用drop方法删除该类
    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop) throws TMDBException {
        String tableName = drop.getName().getName();
        int classId = memConnect.getClassId(tableName);
        drop(classId);
        return true;
    }

    //删除给定类ID的所有相关数据，包括类表、代理类表、双向指针表、切换表和对象表。
    public void drop(int classId) {
        // TODO-task4
        ArrayList<Integer> deputyClassIdList = new ArrayList<>();   // 存储该类对应所有代理类id

        dropClassTable(classId);                            // 1.删除ClassTableItem
        dropDeputyClassTable(classId, deputyClassIdList);   // 2.获取代理类id并在表中删除
        dropBiPointerTable(classId);                        // 3.删除 源类/对象<->代理类/对象 的双向关系表
        dropSwitchingTable(classId);                        // 4.删除switchingTable
        dropObjectTable(classId);                           // 5.删除已创建的源类对象

        // 6.递归删除代理类相关
        // TODO-task4
        //根据收集的代理类ID，从内存表中移除相应的项
        if (deputyClassIdList.isEmpty()) {
            System.out.println("No deputy classes found (id: " + classId + ")");
        } else {
            //遍历代理类ID列表，移除内存表中对应的项
            for (Integer deputyid : deputyClassIdList) {
                MemConnect.getClassTableList().removeIf(item -> item.classid == deputyid);
            }
            System.out.println("Deputy class entries dropped (classId: " + classId + ", Deputy IDs: " + deputyClassIdList + ")");
        }
    }

    /**
     * 给定要删除的class id，删除系统表类表(class table)中的表项
     *
     * @param classId 要删除的表对应的id
     */
    private void dropClassTable(int classId) {
        // TODO-task4
        //从内存表中检索所有记录,尝试移除与指定ID匹配的记录,没有匹配的记录则输出信息
        if (!MemConnect.getClassTableList().removeIf(item -> item.classid == classId)) {
            System.out.println("No class found (id: " + classId + ")");
        } else {
            System.out.println("class dropped (id: " + classId + ")");
        }
    }

    /**
     * 删除系统表中的deputy table，并获取class id对应源类的代理类id
     *
     * @param classId           源类id
     * @param deputyClassIdList 作为返回值，源类对应的代理类id列表
     */
    private void dropDeputyClassTable(int classId, ArrayList<Integer> deputyClassIdList) {
        // TODO-task4
        //遍历代理类表，删除与源类ID匹配的项，同时收集代理类ID
        Iterator<DeputyTableItem> iterator = MemConnect.getDeputyTableList().iterator();
        while (iterator.hasNext()) {
            DeputyTableItem item = iterator.next();// 获取代理类表项
            //如果是源类，则将该项的代理类id添加到代理类id列表中再删除；如果是代理类，则直接删除
            if (item.originid == classId) {
                deputyClassIdList.add(item.deputyid);
                iterator.remove();
            } else if (item.deputyid == classId) {
                iterator.remove();
            }
        }
    }

    /**
     * 删除系统表中的BiPointerTable
     *
     * @param classId 源类id
     */
    private void dropBiPointerTable(int classId) {
        // TODO-task4
        //尝试从bi指针表中移除与给定classId匹配的项，如果没有则输出信息
        if (!MemConnect.getBiPointerTableList().removeIf(item -> item.classid == classId || item.deputyid == classId)) {
            System.out.println("No entries found in BiPointerTable (id: " + classId + ")");
        } else {
            System.out.println("Entries dropped from BiPointerTable (id: " + classId + ")");
        }
    }

    /**
     * 删除系统表中的SwitchingTable
     *
     * @param classId 源类id
     */
    private void dropSwitchingTable(int classId) {
        // TODO-task4
        //同上
        if (!MemConnect.getSwitchingTableList().removeIf(item -> item.oriId == classId || item.deputyId == classId)) {
            System.out.println("No entries found in SwitchingTable (id: " + classId + ")");
        } else {
            System.out.println("Entries dropped from SwitchingTable (id: " + classId + ")");
        }
    }

    /**
     * 删除源类具有的所有对象的列表
     *
     * @param classId 源类id
     */
    private void dropObjectTable(int classId) {
        // TODO-task4
        // 使用MemConnect.getObjectTableList().remove();
        if (!MemConnect.getObjectTableList().removeIf(item -> item.classid == classId)) {
            System.out.println("No entries found in ObjectTable (id: " + classId + ")");
        } else {
            System.out.println("Entries dropped from ObjectTable (id: " + classId + ")");
        }
    }

}
