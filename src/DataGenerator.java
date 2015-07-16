/**
 * Created by chicm on 2015/7/11.
 */

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataGenerator
{
    static Random random = new Random();
    static int numHotel = 10;
    public static void main(String[] args) throws Exception
    {
        if(args.length < 5) {
            System.err.println("Only " + args.length + " arguments supplied, required: 5");
            System.err.println("Usage: DataGenerator SaveFilePath numPerson numHotel numRecord year");
            System.exit(-1);
        }
        SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd");
        int numPerson = Integer.parseInt(args[1]);
        numHotel = Integer.parseInt(args[2]);
        int numRecord = Integer.parseInt(args[3]);
        int year = Integer.parseInt(args[4]);
        if(year < 1900 || year > 2020) {
            return;
        }
        genPersonInfo(numPerson*2);
        genGroups(year);
        groupIndex = 0;
        String strFileName = args[0] + "/hotel";
        FileOutputStream fos=new FileOutputStream(strFileName,false);
        OutputStreamWriter writer = new OutputStreamWriter(fos,"UTF-8");

        for(int i=0;i<numRecord;i++) {
            if(i > 0 && groupIndex < groups.length && (i % 2 == 0)) {
                writer.write(groups[groupIndex++] + "\n");
            } else {
                writer.write(genRegInfo(year) + "\n");
            }
        }
        writer.close();

        OutputStreamWriter writer2 = new OutputStreamWriter(new FileOutputStream(args[0] + "/cand",false),"UTF-8");
        for(int i=0;i<personIDs.length/2;i++) {
            writer2.write(String.format("%s,,,\n", personIDs[i]));
        }
        writer2.close();

        OutputStreamWriter writer3 = new OutputStreamWriter(new FileOutputStream(args[0] + "/groups",false),"UTF-8");
        for(int i=0;i<groups.length;i++) {
            writer3.write(groups[i] + "\n");
        }
        writer3.close();
    }
    static Map<String, Set<Integer>> regMap = new HashMap<>();
    static String[] groups = new String[24];
    static int groupIndex = 0;
    static void genGroups(int year) {
        if(numHotel < 5) {
            return;
        }
        int hotelID = 1;
        int personID = personMap.size()/4;
        if(personID < 3) {
            return;
        }
        for(int i = 0; i < 4; i++) {
            String checkinTime = generateCheckinTime(year);
            for(int j = 0; j < 2; j++) {
                StringBuffer aRecBuf = new StringBuffer();
                aRecBuf.append(personMap.get(personIDs[personID++]) + ",");
                aRecBuf.append(checkinTime);
                aRecBuf.append(",888,");
                aRecBuf.append(generateHotelInfo(hotelID));
                aRecBuf.append(",530102,云南省昆明市五华区,530102410000,昆明市公安局五华分局虹山派出所");
                groups[groupIndex++] = aRecBuf.toString();
            }
            checkinTime = generateCheckinTime(year);
            for(int j = 0; j < 4; j++) {
                StringBuffer aRecBuf = new StringBuffer();
                aRecBuf.append(personMap.get(personIDs[personID++]) + ",");
                aRecBuf.append(checkinTime);
                aRecBuf.append(",888,");
                aRecBuf.append(generateHotelInfo(hotelID));
                aRecBuf.append(",530102,云南省昆明市五华区,530102410000,昆明市公安局五华分局虹山派出所");
                groups[groupIndex++] = aRecBuf.toString();
            }
            hotelID++;
            personID = personMap.size()/4;
        }
    }
    static String genRegInfo(int year)
    {
        StringBuffer aRecBuf = new StringBuffer();
        String id = getPersonID();
        int hotelID = Math.abs(random.nextInt()) % numHotel;
        while(regMap.containsKey(id) && regMap.get(id).contains(hotelID)) {
            id = getPersonID();
            hotelID = Math.abs(random.nextInt()) % numHotel;
        }
        if(!regMap.containsKey(id)) {
            Set<Integer> hotels = new HashSet<>();
            regMap.put(id, hotels);
        }
        regMap.get(id).add(hotelID);

        aRecBuf.append(personMap.get(id) + ",");
        aRecBuf.append(generateCheckinTime(year));
        aRecBuf.append(",888,");
        aRecBuf.append(generateHotelInfo(hotelID));
        aRecBuf.append(",530102,云南省昆明市五华区,530102410000,昆明市公安局五华分局虹山派出所");
        return aRecBuf.toString();
    }

    private static Map<String, String> personMap = new HashMap<>();
    private static String[] personIDs = null;
    static void genPersonInfo(int numPerson) throws Exception{
        SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
        Date DOBBase = f.parse("1971-01-01");
        personIDs = new String[numPerson];
        Set<String> s = new HashSet<>();
        s.add("");
        for(int i = 0; i < numPerson; i++) {
            String DOB =f.format(new java.util.Date(DOBBase.getTime() + ((long)Math.abs(random.nextInt()) % (20*365) *24*3600*1000)));
            boolean sex = random.nextBoolean();
            String id = "";
            while(s.contains(id)) {
                id = String.format("%06d%s%03d%s", Math.abs(random.nextInt() % 1000000), DOB, Math.abs(random.nextInt()) % 1000, (sex ? "1" : "2"));
            }
            s.add(id);
            personIDs[i] = id;
            personMap.put(id, String.format("%s,%s,%s,%s", genName(sex), (sex ? "男" : "女"), id, DOB));
        }
    }
    static String getPersonID() {
        int index = Math.abs(random.nextInt() % personIDs.length);
        return personIDs[index];
    }
    static String[] lastName= {"李", "王", "张", "刘", "陈", "杨", "赵", "黄", "周", "吴",
            "徐", "孙", "胡", "朱", "高", "林", "何", "郭", "马", "罗"};
    static String[] firstNameM = {"宇逸","辰栋","星晨","胤寅","腾中","嘉加","梁锐","锋龙","远运","柔帆","骏运","贤振",
            "子强","栋逸","文震","吉骏","成星","盛骏","畅博","运运","日骏","远振","翱骏","海祥","骞安","晨骞","延晨",
            "泽祥","辰然","辞腾","辰树","腾锦","仕骏","辰运","树泽","晨浩","凯骏","信吉","强祜","运凯","骏鑫","凯琛",
            "强晖","星嘉","家彬","盛运","澄柏","沛栋","睿烁","振梓","祥骏","嘉腾","骏初","博腾","祥嘉","哲稷","泽起","谛佳","允彬","嘉逸"};
    static String[] firstNameF = {"倩璇","淑丽","妍莲","韵雪","美萱","格莉","呈玲","雨淑","静姿","萱慧","俊楠","颖薇","婷初","梓碧","月彩",
            "雪萱","琪萱","晨采","弦璐","珠璇","林雨","雨欣","梅帆","涵美","雪格","优格","帆月","采惠","柏静","紫杉",
            "冰静","彩慧","彩韵","采阳","媛惠","怡雨","菲楠","琪初","月珊","桐莲","琪家","彩漫","洁锦","楠呈","初梦",
            "婷雪","雯云","楠冰","彬薇","雪雪","琬莲","蔚锦","函淑","妮玥","妍梦","橘楠","漫琛","美香","雅梅","露璐" };
    static String genName(boolean male) {
        return lastName[Math.abs(random.nextInt()%lastName.length)] +
                (male ? firstNameM[Math.abs(random.nextInt()%firstNameM.length)]
                : firstNameF[Math.abs(random.nextInt()%firstNameF.length)]);
    }
    static String generateCheckinTime(int year)
    {
        int month = Math.abs(random.nextInt()) % 12 + 1;
        int day = 1;
        switch(month) {
            case 1:
            case 3:
            case 5:
            case 7:
            case 8:
            case 10:
            case 12:
                day = Math.abs(random.nextInt()) % 31 + 1;
                break;
            case 2:
                day = Math.abs(random.nextInt()) % 28 + 1;
                break;
            default:
                day = Math.abs(random.nextInt()) % 30 + 1;
    }
        String checkinDate = String.format("%04d-%02d-%02d", year,month, day );
        return String.format("%s %02d:%02d:%02d", checkinDate,
                Math.abs(random.nextInt()) % 24, Math.abs(random.nextInt())%60, Math.abs(random.nextInt())%60);
    }
    static String generateHotelInfo(int hotelID)
    {
        return String.format("5301810011000%02d,招待所_%02d,虹山南路%02d号", hotelID, hotelID, hotelID);
    }

}