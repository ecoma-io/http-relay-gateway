import { createRouter, createWebHistory } from "vue-router";
import { auth, session } from "./auth";
import AccountsView from "./views/AccountsView.vue";
import DashboardView from "./views/DashboardView.vue";
import LoginView from "./views/LoginView.vue";
import RelaysView from "./views/RelaysView.vue";
import SettingsView from "./views/SettingsView.vue";
import SetupView from "./views/SetupView.vue";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", name: "dashboard", component: DashboardView },
    { path: "/relays", name: "relays", component: RelaysView },
    { path: "/accounts", name: "accounts", component: AccountsView },
    { path: "/settings", name: "settings", component: SettingsView },
    { path: "/setup", name: "setup", component: SetupView },
    { path: "/login", name: "login", component: LoginView },
    { path: "/:pathMatch(.*)*", redirect: "/" },
  ],
});

// Guards run against `session.ready`; the router is only started after the
// first status probe, so no guard ever sees unknown state. `await` inside
// guards is fine: setup and login are once-per-navigation costs.
router.beforeEach(async (to) => {
  if (!session.ready) {
    await auth.refresh();
  }
  if (session.setupRequired) {
    return to.name === "setup" ? true : { name: "setup" };
  }
  if (to.name === "setup") {
    return { name: "dashboard" };
  }
  if (to.name === "login") {
    return session.authed ? { name: "dashboard" } : true;
  }
  if (!session.authed) {
    return { name: "login" };
  }
  return true;
});

export default router;
