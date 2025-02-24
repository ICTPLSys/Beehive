## Backend.cpp 主要服务介绍 

（SimpleBackend.cpp 只是对Backend相关方法的一个简单封装 没有额外增加新的功能）

### 用户注册与登录

1.注册操作：RegisterUser

向hash表新增内容

```c++
states_.username_to_userprofile_map.put(username, user_profile);
```

其中user_profile的定义如下：

```c++
struct UserProfile {
  int64_t user_id;
  std::string first_name;
  std::string last_name;
  std::string salt;
  std::string password_hashed;
 }
```

2.登陆操作：Login

向hash表查询内容：

```c++
auto user_profile_optional = states_.username_to_userprofile_map.get(username);
```

登录会将传入的用户名与密码进行核对验证，验证成功后会返回一个起鉴权作用的签名。

### 帖子相关操作

1.辅助功能：ComposeUrls

​	将原有长的url缩短为更短的url,(用于帖子的美观)：

​	涉及向统一的hash表插入操作，记录了两者的映射：

```
states_.short_to_extended_map.put(target_url.shortened_url,target_url.expanded_url);
```

并且维护了一个vector存储最后生成的全部url

```
std::vector<Url> target_urls;
```

2. ComposeUserMentions

   用于解析转换帖子里被提及的用户（@符号）：

   通过查询哈希表得到参数usernames里的所有用户的信息。

​	3. ComposeText

​        构造了TextServiceReturn对象，记录了该文本的文字内容，url，提及用户的对象。

​    4. ComposePost（发帖子功能）

​     会调用ComposeText 将文本处理成TextServiceReturn逻辑对象。

​     将整数vector的media_ids转换成Media数组对象。

​     会将发帖子写入用户的时间线里。

​     封装构造了一个post，并向内存中的hash表的插入Post。

​	5.ReadPosts

​	 利用hash表查询，将一个post id数组转换为post对象数组

​	6.removePosts

 	该函数的调用是删除起始时间到终止时间的区间里的所有帖子，先通过ReadUserTimeline得到该用户时间段内的所有帖子

​	清除跟帖子所有关联的数据结构

​	 清除该哈希表

```
states_.postid_to_post_map.erase(post.post_id);
```

​		清除所有timeline里跟post相关的

```
(*timeline_optional)->erase(std::make_pair(post.timestamp, post.post_id));
(*mention_timeline_optional)
->erase(std::make_pair(post.timestamp, post.post_id));
(*follower_timeline_optional)->erase(std::make_pair(post.timestamp,post.post_id));
```

​		跟该帖子相关的url哈希表需要清除

```
states_.short_to_extended_map.erase(url.shortened_url);
```

### 时间线读取和写入

1.ReadUserTimeline

查询hash表得到value（treeSet结构）的所有帖子，利用tree的相关方法在时间区间里得到时间线上的所有帖子。

2.WriteUserTimeline

向hash表里的value（treeSet结构）增加构造一个新的Timeline

```
Timeline timeline;
timeline.insert(std::make_pair(timestamp, post_id));
states_.userid_to_usertimeline_map.put(user_id, move(timeline));
```

3.WriteHomeTimeline

首先调用GetFollowers通过user获取用户的followers

然后分别遍历用户提及的列表和followers

对于每个user id,首先查询hash表获取对应的hometimeline(TreeSet结构)，然后将post相关写入进去

4.ReadHomeTimeline

查询hash表得到value（treeSet结构）的所有帖子，利用tree的相关方法在时间区间里得到时间线上的所有帖子。



### 关注和取关

1.Follow

主要是向Followers和followee的哈希表的value增加一项

2.Unfollow

主要是向Followers和followee的哈希表的value删除一项

3.GetFollowers 

GetFollowees直接查询哈希表即可



### 计算哈希表大小

主要作用是计算state里各个哈希表数据结构的最大大小、平均大小、最小大小



## 主要数据结构介绍

主要数据结构均存储在states.hpp文件中 

主要数据结构为哈希表，如下代码所示

```c
ConcurrentHashMap<std::string, UserProfile> username_to_userprofile_map;
  ConcurrentHashMap<std::string, std::string> filename_to_data_map;
  ConcurrentHashMap<std::string, std::string> short_to_extended_map;
  ConcurrentHashMap<int64_t, Timeline> userid_to_hometimeline_map;
  ConcurrentHashMap<int64_t, Timeline> userid_to_usertimeline_map;
  ConcurrentHashMap<int64_t, Post> postid_to_post_map;
  ConcurrentHashMap<int64_t, std::set<int64_t>> userid_to_followers_map;
  ConcurrentHashMap<int64_t, std::set<int64_t>>  userid_to_followees_map;
```

username_to_userprofile_map 是 由用户名到用户个人信息对象的映射。

filename_to_data_map 是跟Media相关的，相当于上传了文件，存储了文件名和文件数据的映射。

short_to_extended_map存储了url缩写到全拼的映射。

userid_to_usertimeline_map，userid_to_hometimeline_map均是用户id到相应的timeline的映射，timeline是一个tree，tree里面存储的结构是一个个pair<>,包含了帖子的时间戳和id

postid_to_post_map存储了帖子id到帖子对象映射。

userid_to_followers_map，userid_to_followees_map描述了一个图，及社交网络图，描述了各个用户之间的互相关注、被关注的关系



## 多线程服务介绍

### Server 介绍

Server的函数主要定义在main.cpp 的service(Request *req, SimpleBackEndServer &backend)函数中。

service函数的逻辑就是根据Request实际的不同类型，调用backend相应的不同函数处理，并且将返回结果统一封装为ServiceResult

### client 介绍

client的主要函数是gen_random_req，是通过一个随机数，在不同随机数下产生不同的请求，如ReadHomeTimelineReq ， ComposePostReq ， RemovePostsReq， FollowReq等，其中Request里的内容也是完全随机的。

### 多线程实现

多线程采用了生产者，消费者模式实现，通过无锁队列using RequestQueue = boost::lockfree::queue<Request *, boost::lockfree::fixed_sized<false>>;

多个client向队列中加入请求，而server从队列中取出请求并处理。



## 主函数介绍

### 数据读入

数据文件主要描述了一个社交网络图，及各个用户之间的关注与被关注关系，读入数据后构造相应的请求，首先是注册用户请求，即构造图的顶点，然后是Follow请求，构造图的边。然后随机性的构造了一些帖子。

### 多线程

接着构造相应的多个生产者消费者，以及单个消费者线程，等到所有线程结束后，完成任务。