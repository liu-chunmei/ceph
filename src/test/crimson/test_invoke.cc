
#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>

struct Message {
  int index = 0;
};
class Service {
  int last_index = 0;
public:
  void compare_index(int i) {
    assert(i > last_index);
    last_index = i;
  }
};
class TInvoke {
  seastar::sharded<Service> srv;
public:
  seastar::future<> start(){
    return srv.start();
  }
  seastar::future<> stop(){
    return srv.stop();
  }
  seastar::future<> test(Message &&msg){
      return srv.invoke_on(1, [m = std::move(msg)](auto local_srv) {
        local_srv.compare_index(m.index);
      });
  }
};
int main(int argc, char** argv)
{
  seastar::app_template::config app_cfg;
  app_cfg.name = "seastar-invoke-test";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));
  const char *bootstrap_args[] = {argv[0], "--smp", "3" };
  TInvoke inv;
  return app.run(
    sizeof(bootstrap_args) / sizeof(bootstrap_args[0]),
    const_cast<char**>(bootstrap_args), 
    [&inv]{
    return inv.start().then([&inv] {
      for (int i = 1; i <= 10000; i++){
        struct Message msg;
        msg.index = i;
        std::ignore = inv.test(std::move(msg));
      }
      return seastar::sleep(std::chrono::seconds(1));
    }).then([&inv] {
      return inv.stop();
    });
  });  
}
