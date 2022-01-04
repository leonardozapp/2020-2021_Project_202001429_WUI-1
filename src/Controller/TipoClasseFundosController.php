<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * TipoClasseFundos Controller
 *
 * @property \App\Model\Table\TipoClasseFundosTable $TipoClasseFundos
 * @method \App\Model\Entity\TipoClasseFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class TipoClasseFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $tipoClasseFundos = $this->paginate($this->TipoClasseFundos);

        $this->set(compact('tipoClasseFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Tipo Classe Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $tipoClasseFundo = $this->TipoClasseFundos->get($id, [
            'contain' => ['CadastroFundos'],
        ]);

        $this->set(compact('tipoClasseFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $tipoClasseFundo = $this->TipoClasseFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $tipoClasseFundo = $this->TipoClasseFundos->patchEntity($tipoClasseFundo, $this->request->getData());
            if ($this->TipoClasseFundos->save($tipoClasseFundo)) {
                $this->Flash->success(__('The tipo classe fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo classe fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoClasseFundo'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Tipo Classe Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $tipoClasseFundo = $this->TipoClasseFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $tipoClasseFundo = $this->TipoClasseFundos->patchEntity($tipoClasseFundo, $this->request->getData());
            if ($this->TipoClasseFundos->save($tipoClasseFundo)) {
                $this->Flash->success(__('The tipo classe fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo classe fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoClasseFundo'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Tipo Classe Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $tipoClasseFundo = $this->TipoClasseFundos->get($id);
        if ($this->TipoClasseFundos->delete($tipoClasseFundo)) {
            $this->Flash->success(__('The tipo classe fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The tipo classe fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
