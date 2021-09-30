package org.fretron.avroPractice.service

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.fretron.avroPractice.model.User
import java.io.File


class UserService {

    private val user1 = User()
    val user2 = User(1, "Mark", "mark@gmail.com", 24)
    val user3 = User(2, "Roy", "roy.jason@gmail.com", 29)

    init {
        user1.setId(100)
        user1.setName("Virendra")
        user1.setEmail("vpsj@gmail.com")
        user1.setAge(23)
    }

    fun serializeUser(){

        val datumWriter = SpecificDatumWriter<User>()
        val dataFileWriter = DataFileWriter<User>(datumWriter)

        dataFileWriter.create(user1.schema, File("./src/org/fretron/avroPractice/serializedUserFiles/User.avro"))

        dataFileWriter.append(user1)
        dataFileWriter.append(user2)
        dataFileWriter.append(user3)

        dataFileWriter.close()
    }

    fun deserializeUser() : MutableList<User>{

        val datumReader = SpecificDatumReader<User>()
        val datafileReader = DataFileReader<User>(File("./src/org/fretron/avroPractice/serializedUserFiles/User.avro"),datumReader)

        val userList = mutableListOf<User>()
        while(datafileReader.hasNext()){
            val user = datafileReader.next()
            userList.add(user)
        }
            return userList
    }
}

fun main(){
    val userService = UserService()
    userService.serializeUser()
    val userList = userService.deserializeUser()
    println(userList)
}