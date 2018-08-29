package com.cm.data.util

import java.util.Properties

object DBHelper {

  def setConnectionProperty(): Properties = {
    val connProp = new Properties
    connProp.put("driver", "com.mysql.cj.jdbc.Driver")
    connProp.put("user", System.getProperty("db_user"))
    connProp.put("password", System.getProperty("db_password"))

    connProp
  }

}
