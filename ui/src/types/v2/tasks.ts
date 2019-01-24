import {Organization, Task as TaskAPI} from 'src/api'

export interface Task extends TaskAPI {
  organization: Organization
}
