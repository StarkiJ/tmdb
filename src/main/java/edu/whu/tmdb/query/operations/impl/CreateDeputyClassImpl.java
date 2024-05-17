package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.List;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.query.operations.CreateDeputyClass;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;


public class CreateDeputyClassImpl implements CreateDeputyClass {
    private final MemConnect memConnect;

    public CreateDeputyClassImpl() { this.memConnect = MemConnect.getInstance(MemManager.getInstance()); }

    @Override
    public boolean createDeputyClass(Statement stmt) throws TMDBException, IOException {
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }

    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) throws TMDBException, IOException {
        // 1.获取代理类名、代理类型、select元组
        String deputyClassName = stmt.getDeputyClass().toString();  // 代理类名
        if (memConnect.classExist(deputyClassName)) {
            throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, deputyClassName);
        }
        int deputyType = getDeputyType(stmt);   // 代理类型
        Select selectStmt = stmt.getSelect();
        SelectResult selectResult = getSelectResult(selectStmt);

        // 2.执行代理类创建
        return createDeputyClassStreamLine(selectResult, deputyType, deputyClassName);
    }

    public boolean createDeputyClassStreamLine(SelectResult selectResult, int deputyType, String deputyClassName) throws TMDBException, IOException {
        int deputyId = createDeputyClass(deputyClassName, selectResult, deputyType);
        createDeputyTableItem(selectResult.getClassName(), deputyType, deputyId);
        createBiPointerTableItem(selectResult, deputyId);
        return true;
    }

    /**
     * 创建代理类的实现，包含代理类classTableItem的创建和switchingTableItem的创建
     * @param deputyClassName 代理类名称
     * @param selectResult 代理类包含的元组列表
     * @param deputyRule 代理规则
     * @return 新建代理类ID
     */
    private int createDeputyClass(String deputyClassName, SelectResult selectResult, int deputyRule) throws TMDBException {
        // TODO-task3
        // 1.生成新的classId
        MemConnect.getClassTable().maxid++;
        int classId = MemConnect.getClassTable().maxid;//类ID

        int attrNum=selectResult.getAttrid().length;//类属性总个数
        int oriId;//源类ID
        int oriAttrId;//源类属性ID
        String attrName;//当前属性名
        String attrType;//当前属性类型
        String oriAttrName;//源类属性名

        // 2.遍历selectResult，创建ClassTableItem并添加到ClassTableList
        for (int i = 0; i < attrNum; i++) {

            attrName = selectResult.getAttrname()[i];//当前属性名
            attrType = selectResult.getType()[i];//当前属性类型

            //（类名，类ID，类属性总个数，当前属性ID，当前属性名，当前属性类型，类属性，alias）
            ClassTableItem classTableItem = new ClassTableItem(deputyClassName,classId,attrNum,
                    i,attrName,attrType, "2","");//1为源类2表示代理类From ClassTyepe.java

            MemConnect.getClassTableList().add(classTableItem);

            // 3.创建SwitchingTableItem并添加到SwitchingTableList
            oriId=memConnect.getClassId(selectResult.getClassName()[i]);
            oriAttrId=selectResult.getAttrid()[i];
            oriAttrName=selectResult.getAttrname()[i];

            //（源类ID，源类属性ID，源类属性名称，代理类ID，代理类属性ID，代理类属性名称，代理规则）
            SwitchingTableItem switchingTableItem = new SwitchingTableItem(oriId,oriAttrId,oriAttrName,
                    classId,i, attrName,Integer.toString(deputyRule));//将int转变成string

            MemConnect.getSwitchingTableList().add(switchingTableItem);
        }
        return classId;

        //return -1;
    }

    /**
     * 新建deputyTableItem
     * @param classNames 源类类名列表
     * @param deputyType 代理规则
     * @param deputyId 代理类id
     */
    public void createDeputyTableItem(String[] classNames, int deputyType, int deputyId) throws TMDBException {
        // TODO-task3
        // 使用MemConnect.getDeputyTableList().add()
        HashSet<String> oriNames = Arrays.stream(classNames).collect(Collectors.toCollection(HashSet::new));//去除重复元素
        for (String className : oriNames) {
            int classId = memConnect.getClassId(className);
            DeputyTableItem deputyTableItem = new DeputyTableItem(classId,deputyId,new String[]{Integer.toString(deputyType)});//int to string to string[]
            MemConnect.getDeputyTableList().add(deputyTableItem);
        }
    }

    /**
     * 插入元组，并新建BiPointerTableItem
     * @param selectResult 插入的元组列表
     * @param deputyId 新建代理类id
     */
    private void createBiPointerTableItem(SelectResult selectResult, int deputyId) throws TMDBException, IOException {
        // TODO-task3
        // 使用insert.execute()插入对象

        // 可调用getOriginClass(selectResult);

        // 使用MemConnect.getBiPointerTableList().add()插入BiPointerTable
        InsertImpl insert = new InsertImpl();
        List<String> columns= Arrays.asList(selectResult.getAttrname());//将返回的字符串数组转换为列表
        for (Tuple tuple : selectResult.getTpl().tuplelist) {
            //void execute(String tableName, List<String> columns, TupleList tupleList)
            int deputyTupleId = insert.execute(deputyId, columns, new Tuple(tuple.tuple));//代理类对象ID
            HashSet<Integer> origin = getOriginClass(selectResult);//去除重复元素
            for(int ori:origin) {
                // 插入新的BiPointerTableItem
                int oriId = memConnect.getClassId(selectResult.getClassName()[ori]);//源类ID
                int oriTupleId = tuple.tupleIds[ori];//源类对象ID
                //(源类ID，源类对象ID，代理类ID，代理对象ID）
                BiPointerTableItem biPointerTableItem = new BiPointerTableItem(oriId, oriTupleId, deputyId, deputyTupleId);
                MemConnect.getBiPointerTableList().add(biPointerTableItem);
            }
        }
    }

    /**
     * 给定创建代理类语句，返回代理规则
     * @param stmt 创建代理类语句
     * @return 代理规则
     */
    private int getDeputyType(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) {
        switch (stmt.getType().toLowerCase(Locale.ROOT)) {
            case "selectdeputy":    return 0;
            case "joindeputy":      return 1;
            case "uniondeputy":     return 2;
            case "groupbydeputy":   return 3;
        }
        return -1;
    }

    /**
     * 给定查询语句，返回select查询执行结果（创建deputyclass后面的select语句中的selectResult）
     * @param selectStmt select查询语句
     * @return 查询执行结果（包含所有满足条件元组）
     */
    private SelectResult getSelectResult(Select selectStmt) throws TMDBException, IOException {
        SelectImpl selectExecutor = new SelectImpl();
        return selectExecutor.select(selectStmt);
    }

    private HashSet<Integer> getOriginClass(SelectResult selectResult) {
        ArrayList<String> collect = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(ArrayList::new));
        HashSet<String> collect1 = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(HashSet::new));
        HashSet<Integer> res = new HashSet<>();
        for (String s : collect1) {
            res.add(collect.indexOf(s));
        }
        return res;
    }
}
