package com.mashibing.flinkjava.code.chapter6.serializer;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StudentSerializer extends Serializer {

    @Override
    public void write(Kryo kryo, Output output, Object o) {
       Student student = (Student)o;
       output.writeInt(student.stuId);
        output.writeString(student.name);
        output.writeInt(student.age);
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        Student stu = new Student();
        stu.stuId = input.readInt();
        stu.name = input.readString();
        stu.age = input.readInt();
        return stu;
    }
}
