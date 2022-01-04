<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CadastroFundo $cadastroFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Cadastro Fundo'), ['action' => 'edit', $cadastroFundo->cnpj_fundo_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Cadastro Fundo'), ['action' => 'delete', $cadastroFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $cadastroFundo->cnpj_fundo_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Cadastro Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Cadastro Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="cadastroFundos view content">
            <h3><?= h($cadastroFundo->cnpj_fundo_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Cnpj Fundo') ?></th>
                    <td><?= $cadastroFundo->has('cnpj_fundo') ? $this->Html->link($cadastroFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $cadastroFundo->cnpj_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipo Classe Fundo') ?></th>
                    <td><?= $cadastroFundo->has('tipo_classe_fundo') ? $this->Html->link($cadastroFundo->tipo_classe_fundo->id, ['controller' => 'TipoClasseFundos', 'action' => 'view', $cadastroFundo->tipo_classe_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipo Rentabilidade Fundo') ?></th>
                    <td><?= $cadastroFundo->has('tipo_rentabilidade_fundo') ? $this->Html->link($cadastroFundo->tipo_rentabilidade_fundo->id, ['controller' => 'TipoRentabilidadeFundos', 'action' => 'view', $cadastroFundo->tipo_rentabilidade_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Gestor Fundo') ?></th>
                    <td><?= $cadastroFundo->has('gestor_fundo') ? $this->Html->link($cadastroFundo->gestor_fundo->id, ['controller' => 'GestorFundos', 'action' => 'view', $cadastroFundo->gestor_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Administrador Fundo') ?></th>
                    <td><?= $cadastroFundo->has('administrador_fundo') ? $this->Html->link($cadastroFundo->administrador_fundo->id, ['controller' => 'AdministradorFundos', 'action' => 'view', $cadastroFundo->administrador_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('DT CONST') ?></th>
                    <td><?= h($cadastroFundo->DT_CONST) ?></td>
                </tr>
                <tr>
                    <th><?= __('DT INI CLASSE') ?></th>
                    <td><?= h($cadastroFundo->DT_INI_CLASSE) ?></td>
                </tr>
                <tr>
                    <th><?= __('DT REG CVM') ?></th>
                    <td><?= h($cadastroFundo->DT_REG_CVM) ?></td>
                </tr>
                <tr>
                    <th><?= __('CONDOM ABERTO') ?></th>
                    <td><?= $cadastroFundo->CONDOM_ABERTO ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('FUNDO COTAS') ?></th>
                    <td><?= $cadastroFundo->FUNDO_COTAS ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('FUNDO EXCLUSIVO') ?></th>
                    <td><?= $cadastroFundo->FUNDO_EXCLUSIVO ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('INVEST QUALIF') ?></th>
                    <td><?= $cadastroFundo->INVEST_QUALIF ? __('Yes') : __('No'); ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
