<template>
  <v-app>
    <v-app-bar
      app
      color="primary"
      dark
    >
      <div class="d-flex align-center">
        <h1>Command Runner</h1>
      </div>

      <v-spacer></v-spacer>

    </v-app-bar>

    <v-main>
      <CommandRunner/>
    </v-main>
  </v-app>
</template>

<script>
import CommandRunner from './components/CommandRunner';

export default {
  name: 'App',

  components: {
    CommandRunner,
  },

  data: () => ({
    //
  }),
};
</script>
