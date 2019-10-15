// Libraries
import React, {SFC} from 'react'
// Components
import DataExplorer from 'src/dataExplorer/components/DataExplorer'
import {Page} from '@influxdata/clockface'
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'
import VisOptionsButton from 'src/timeMachine/components/VisOptionsButton'
import ViewTypeDropdown from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'
import TimeZoneDropdown from 'src/shared/components/TimeZoneDropdown'
import DeleteDataButton from 'src/dataExplorer/components/DeleteDataButton'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

const DataExplorerPage: SFC = ({children}) => {
  return (
    <Page titleTag={pageTitleSuffixer(['Data Explorer'])}>
      {children}
      <GetResources resource={ResourceType.Variables}>
        <Page.Header fullWidth={true}>
          <Page.Header.Left>
            <PageTitleWithOrg title="Data Explorer" />
          </Page.Header.Left>
          <Page.Header.Right>
            <DeleteDataButton />
            <TimeZoneDropdown />
            <ViewTypeDropdown />
            <VisOptionsButton />
            <SaveAsButton />
          </Page.Header.Right>
        </Page.Header>
        <Page.Contents fullWidth={true} scrollable={false}>
          <DataExplorer />
        </Page.Contents>
      </GetResources>
    </Page>
  )
}

export default DataExplorerPage