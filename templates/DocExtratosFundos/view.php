<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\DocExtratosFundo $docExtratosFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Doc Extratos Fundo'), ['action' => 'edit', $docExtratosFundo->cnpj_fundo_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Doc Extratos Fundo'), ['action' => 'delete', $docExtratosFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $docExtratosFundo->cnpj_fundo_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Doc Extratos Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Doc Extratos Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="docExtratosFundos view content">
            <h3><?= h($docExtratosFundo->cnpj_fundo_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Cnpj Fundo') ?></th>
                    <td><?= $docExtratosFundo->has('cnpj_fundo') ? $this->Html->link($docExtratosFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $docExtratosFundo->cnpj_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipo Anbima Class') ?></th>
                    <td><?= $docExtratosFundo->has('tipo_anbima_class') ? $this->Html->link($docExtratosFundo->tipo_anbima_class->id, ['controller' => 'TipoAnbimaClasses', 'action' => 'view', $docExtratosFundo->tipo_anbima_class->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('APLIC MIN') ?></th>
                    <td><?= $this->Number->format($docExtratosFundo->APLIC_MIN) ?></td>
                </tr>
                <tr>
                    <th><?= __('QT DIA PAGTO COTA') ?></th>
                    <td><?= $this->Number->format($docExtratosFundo->QT_DIA_PAGTO_COTA) ?></td>
                </tr>
                <tr>
                    <th><?= __('QT DIA PAGTO RESGATE') ?></th>
                    <td><?= $this->Number->format($docExtratosFundo->QT_DIA_PAGTO_RESGATE) ?></td>
                </tr>
                <tr>
                    <th><?= __('PR COTA ETF MAX') ?></th>
                    <td><?= $this->Number->format($docExtratosFundo->PR_COTA_ETF_MAX) ?></td>
                </tr>
                <tr>
                    <th><?= __('DT COMPTC') ?></th>
                    <td><?= h($docExtratosFundo->DT_COMPTC) ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
